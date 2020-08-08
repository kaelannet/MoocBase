package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transEntry = transactionTable.get(transNum);
        Long prevLSN = transEntry.lastLSN;
        LogRecord log = new CommitTransactionLogRecord(transNum, prevLSN);
        Long lsn = logManager.appendToLog(log);
        // Update the Transaction Table
        transEntry.lastLSN = lsn;
        transEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        logManager.flushToLSN(lsn);
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transEntry = transactionTable.get(transNum);
        Long prevLSN = transEntry.lastLSN;
        LogRecord log = new AbortTransactionLogRecord(transNum, prevLSN);
        Long lsn = logManager.appendToLog(log);
        // Update the Transaction Table
        transEntry.lastLSN = lsn;
        transEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        TransactionTableEntry transEntry = transactionTable.get(transNum);
        Long lastLSN = transactionTable.get(transNum).lastLSN;
        LogRecord lastLog = logManager.fetchLogRecord(lastLSN);
        if (transEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            lastLSN = rollbackUndo(lastLog, lastLSN, -1L);
        }
        LogRecord endRecord = new EndTransactionLogRecord(transNum, lastLSN);
        Long endLSN = logManager.appendToLog(endRecord);
        transEntry.lastLSN = endLSN;
        transEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return endLSN;
    }

    public Long rollbackUndo(LogRecord log, Long lastLSN, Long stopPoint) {
        LogRecord record = log;
        while (record != null) {
            if (record.isUndoable()) {
                Pair<LogRecord, Boolean> clr = record.undo(lastLSN);
                Long clrLSN = logManager.appendToLog(clr.getFirst());
                if (clr.getSecond()) {
                    logManager.flushToLSN(clrLSN);
                }
                clr.getFirst().redo(diskSpaceManager, bufferManager);
                // Update DPTable
                if (clr.getFirst().type.equals(LogType.UNDO_UPDATE_PAGE)) {
                    dirtyPageTable.putIfAbsent(clr.getFirst().getPageNum().get(), clrLSN);
                }
                if (clr.getFirst().type.equals(LogType.UNDO_ALLOC_PAGE)) {
                    dirtyPageTable.remove(clr.getFirst().getPageNum().get());
                }
                // Update the last LSN to the LSN of the newly created CLR
                lastLSN = clrLSN;
            }
            // Iterate to the next record to call undo
            if (record.getUndoNextLSN().isPresent() && record.getUndoNextLSN().get() >  stopPoint) {
                record = logManager.fetchLogRecord(record.getUndoNextLSN().get());
            } else if (record.getPrevLSN().isPresent() && !record.getUndoNextLSN().isPresent() && record.getPrevLSN().get() > stopPoint) {
                record = logManager.fetchLogRecord(record.getPrevLSN().get());
            } else {
                break;
            }
        }
        return lastLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        Long prevLSN = transactionTable.get(transNum).lastLSN;
        if (after.length > bufferManager.EFFECTIVE_PAGE_SIZE/2) {
            LogRecord undoOnly = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, null);
            Long undoLSN = logManager.appendToLog(undoOnly);
            LogRecord redoOnly = new UpdatePageLogRecord(transNum, pageNum, undoLSN, pageOffset, null, after);
            prevLSN = logManager.appendToLog(redoOnly);
        } else {
            LogRecord log = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            prevLSN = logManager.appendToLog(log);
        }
        // update transaction and dirty page tables
        transactionTable.get(transNum).lastLSN = prevLSN;
        dirtyPageTable.putIfAbsent(pageNum, prevLSN);
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RUNNING);
        transactionTable.get(transNum).touchedPages.add(pageNum);
        return prevLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepoint = transactionEntry.getSavepoint(name);
        TransactionTableEntry transEntry = transactionTable.get(transNum);
        Long lastLSN = transEntry.lastLSN;
        LogRecord lastLog = logManager.fetchLogRecord(lastLSN);
        rollbackUndo(lastLog, lastLSN, savepoint);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }
        copyDPT(dpt, touchedPages, txnTable, numTouchedPages);
        copyTXN(dpt, touchedPages, txnTable, numTouchedPages);

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    public void copyTXN(Map<Long, Long> dpt, Map<Long, List<Long>> touchedPages,
                           Map<Long, Pair<Transaction.Status, Long>> txnTable, int numTouchedPages) {
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            boolean fits;
            fits = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size() + 1, touchedPages.size(),
                    numTouchedPages);
            if (fits) {
                Pair<Transaction.Status, Long> value = new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN);
                txnTable.put(entry.getKey(), value);
            } else {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
        }
    }

    public void copyDPT(Map<Long, Long> dpt, Map<Long, List<Long>> touchedPages,
                           Map<Long, Pair<Transaction.Status, Long>> txnTable, int numTouchedPages) {
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            boolean fits;
            fits = EndCheckpointLogRecord.fitsInOneRecord(dpt.size() + 1, txnTable.size(), touchedPages.size(),
                    numTouchedPages);
            if (fits) {
                dpt.put(entry.getKey(), entry.getValue());
            } else {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
        }

    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        restartAnalysis();
        restartRedo();
        this.bufferManager.iterPageNums((pageNum, isDirty) ->
        {if (!isDirty) this.dirtyPageTable.remove(pageNum);});
        return () -> {restartUndo(); checkpoint();};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        LogRecord log;
        while(iter.hasNext()) {
            log = iter.next();
            Long transactionCounter = getTransactionCounter.get();

            if (log.type.equals(LogType.BEGIN_CHECKPOINT)) {
                updateTransactionCounter.accept(Math.max(transactionCounter, log.getMaxTransactionNum().get()));
            } else if (log.type.equals(LogType.END_CHECKPOINT)) {
                for(Map.Entry<Long, Long> entry : log.getDirtyPageTable().entrySet()) {
                    dirtyPageTable.put(entry.getKey(), entry.getValue());
                }
                Map<Long, Pair<Transaction.Status, Long>> checkpointTransTable = log.getTransactionTable();
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : checkpointTransTable.entrySet()) {
                    Long transNum = entry.getKey();
                    Transaction trans;
                    Long checkpointLSN = entry.getValue().getSecond();
                    int checkpointStatus = entry.getValue().getFirst().getValue();
                    if (transactionTable.get(transNum) != null) {
                        trans = transactionTable.get(transNum).transaction;
                    } else {
                        trans = newTransaction.apply(transNum);
                        startTransaction(trans);
                    }
                    if (transactionTable.get(transNum).lastLSN < checkpointLSN) {
                        transactionTable.get(transNum).lastLSN = checkpointLSN;
                    }
                    int recordedStatus = transactionTable.get(transNum).transaction.getStatus().getValue();
                    // If the checkpoint status value is greater than the recorded status value then the checkpoint
                    // status is more current, and we should update the table
                    if (checkpointStatus > recordedStatus) {
                        if ( entry.getValue().getFirst().equals(Transaction.Status.ABORTING)) {
                            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else {
                            transactionTable.get(transNum).transaction.setStatus(entry.getValue().getFirst());
                        }
                    }
                }
                for (Map.Entry<Long, List<Long>> entry : log.getTransactionTouchedPages().entrySet()) {
                    long transNum = entry.getKey();
                    TransactionTableEntry transEnt = this.transactionTable.get(transNum);
                    // if transaction is complete, continue;
                    if (transEnt.transaction.getStatus() == Transaction.Status.COMPLETE)
                        continue;
                    for (Long pageNum : entry.getValue()) {
                        transEnt.touchedPages.add(pageNum);
                        acquireTransactionLock(transEnt.transaction, getPageLockContext(pageNum), LockType.X);
                    }
                }
            }
            // Get transaction or create transaction if not present in table
            Transaction transaction;
            if (log.getTransNum().isPresent()) {
                Long transNum = log.getTransNum().get();
                if (transactionTable.get(transNum) != null) {
                    transaction = transactionTable.get(transNum).transaction;
                } else {
                    transaction = newTransaction.apply(transNum);
                    startTransaction(transaction);
                }
                if (log.getPageNum().isPresent()) {
                    TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                    transactionEntry.touchedPages.add(log.getPageNum().get());
                    LockContext pageContext = getPageLockContext(log.getPageNum().get());
                    acquireTransactionLock(transaction, pageContext, LockType.X);
                    if (log.type.equals(LogType.UNDO_ALLOC_PAGE) || log.type.equals(LogType.FREE_PAGE)) {
                        dirtyPageTable.remove(log.getPageNum().get());
                    } else if (log.type.equals(LogType.UNDO_UPDATE_PAGE) || log.type.equals(LogType.UPDATE_PAGE)) {
                        dirtyPageTable.putIfAbsent(log.getPageNum().get(), log.LSN);
                    }
                }
                if (transactionTable.get(transNum).lastLSN < log.getLSN()) {
                    transactionTable.get(transNum).lastLSN = log.LSN;
                }
                if (log.type.equals(LogType.END_TRANSACTION)) {
                    transaction.cleanup();
                    transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                }
                if (log.type.equals(LogType.ABORT_TRANSACTION)) {
                    transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);

                }
                if (log.type.equals(LogType.COMMIT_TRANSACTION)) {
                    transaction.setStatus(Transaction.Status.COMMITTING);
                }
            }
        }
        for (Map.Entry<Long, TransactionTableEntry> table : transactionTable.entrySet()) {
            TransactionTableEntry entry = table.getValue();
            if (entry.transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                entry.transaction.cleanup();
                LogRecord end = new EndTransactionLogRecord(table.getKey(), entry.lastLSN);
                entry.lastLSN = logManager.appendToLog(end);
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(table.getKey());
            }
            if (entry.transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                LogRecord abortRecord = new AbortTransactionLogRecord(table.getKey(), entry.lastLSN);
                entry.lastLSN = this.logManager.appendToLog(abortRecord);
                entry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        Long minLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> lsnIterator = logManager.scanFrom(minLSN);
        while (lsnIterator.hasNext()) {
            LogRecord currLog = lsnIterator.next();
            if (currLog.isRedoable()) {
                LogType type = currLog.type;
                if (type == LogType.FREE_PAGE || type == LogType.ALLOC_PAGE || type == LogType.UPDATE_PAGE ||
                        type == LogType.UNDO_UPDATE_PAGE || type == LogType.UNDO_ALLOC_PAGE || type == LogType.UNDO_FREE_PAGE) {
                    if (dirtyPageTable.values().contains(currLog.LSN)) {
                        if (dirtyPageTable.get(currLog.getPageNum().get()) <= currLog.getLSN()) {
                            LockContext parentContext = getPageLockContext(currLog.getPageNum().get()).parentContext();
                            Page page = bufferManager.fetchPage(parentContext, currLog.getPageNum().get(),  true);
                            if (page.getPageLSN() < currLog.getLSN()) {
                                currLog.redo(diskSpaceManager, bufferManager);
                            }
                        }
                    }
                } else if (type == LogType.UNDO_ALLOC_PART || type == LogType.ALLOC_PART
                || type == LogType.FREE_PART || type == LogType.UNDO_FREE_PART) {
                    currLog.redo(diskSpaceManager, bufferManager);
                }
            }


        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        PriorityQueue<Pair<Long, LogRecord>> pQueue = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (entry.getValue().transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                Long lsn = entry.getValue().lastLSN;
                LogRecord log = logManager.fetchLogRecord(lsn);
                Pair<Long, LogRecord> container = new Pair<>(lsn, log);
                pQueue.add(container);
            }
        }
        while (!pQueue.isEmpty()) {
            Pair<Long, LogRecord> data = pQueue.poll();
            LogRecord log = data.getSecond();
            Long logLSN = data.getFirst();
            Long transNum = log.getTransNum().get();
            if (log.isUndoable()) {
                Pair<LogRecord, Boolean> clr = log.undo(transactionTable.get(transNum).lastLSN);
                Long clrLSN = logManager.appendToLog(clr.getFirst());
                transactionTable.get(transNum).lastLSN = clrLSN;
                if (clr.getSecond()) {
                    logManager.flushToLSN(clrLSN);
                }
                clr.getFirst().redo(diskSpaceManager, bufferManager);
                // Update DPTable
                if (clr.getFirst().type.equals(LogType.UNDO_UPDATE_PAGE)) {
                    dirtyPageTable.putIfAbsent(clr.getFirst().getPageNum().get(), clrLSN);
                }
                if (clr.getFirst().type.equals(LogType.UNDO_ALLOC_PAGE)) {
                    dirtyPageTable.remove(clr.getFirst().getPageNum().get());
                }
            }
            Long nextLSN = 0L;
            if (log.getUndoNextLSN().isPresent()) {
                nextLSN = log.getUndoNextLSN().get();
            } else if (log.getPrevLSN().isPresent()) {
                nextLSN = log.getPrevLSN().get();
            } if (nextLSN == 0L) {
                LogRecord end = new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
                transactionTable.get(transNum).lastLSN = logManager.appendToLog(end);
                transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
            } else {
                Pair<Long, LogRecord> pair = new Pair<>(nextLSN, logManager.fetchLogRecord(nextLSN));
                pQueue.add(pair);
            }
        }
        return;
    }


    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
