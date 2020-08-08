package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            for (Lock lock : locks) {
                if (!LockType.compatible(lock.lockType, lockType) && lock.transactionNum != except) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            Long currentTransaction = lock.transactionNum;
            ResourceName name = lock.name;
            int i = 0;
            for (Lock l : locks) {
                if (l.transactionNum == currentTransaction) {
                    locks.set(i, lock);
                    List<Lock> transLocks = transactionLocks.get(currentTransaction);
                    int index = transLocks.indexOf(l);
                    transLocks.set(index, lock);
                    return;
                }
                i++;
            }
            locks.add(lock);
            transactionLocks.putIfAbsent(currentTransaction, new ArrayList<>());
            transactionLocks.get(currentTransaction).add(lock);
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            locks.remove(lock);
            List<Lock> transLocks = transactionLocks.get(lock.transactionNum);
            transLocks.remove(lock);
            processQueue();
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            while (!waitingQueue.isEmpty()) {
                Long except = waitingQueue.peek().lock.transactionNum;
                LockType type = waitingQueue.peek().lock.lockType;
                if (checkCompatible(type, except)) {
                    LockRequest request = waitingQueue.pollFirst();
                    grantOrUpdateLock(request.lock);
                    request.transaction.unblock();
                    for (Lock release : request.releasedLocks) {
                        releaseLock(release);
                    }
                } else {
                    break;
                }
            }
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }


    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        synchronized (this) {
            Long currentTransaction = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, currentTransaction);
            ResourceEntry resource = getResourceEntry(name);
            LockType oldLockType = resource.getTransactionLockType(currentTransaction);
            // NoLockHeld error check
            for (ResourceName rname : releaseLocks) {
                ResourceEntry releaseEntry = getResourceEntry(rname);
                LockType releaseType = releaseEntry.getTransactionLockType(currentTransaction);
                if (releaseType.equals(LockType.NL)) {
                    throw new NoLockHeldException("Release Lock isn't held");
                }
            }
            // DuplicateLock Error check
            if (!releaseLocks.contains(name) && !oldLockType.equals(LockType.NL)) {
                throw new DuplicateLockRequestException("Lock already held by transaction");
            }
            // If lock is compatible, grant or update and release locks
            if (resource.checkCompatible(lockType, currentTransaction)) {
                resource.grantOrUpdateLock(newLock);
                for (ResourceName releaseName : releaseLocks) {
                    ResourceEntry releaseResource = getResourceEntry(releaseName);
                    LockType releaseLockType = releaseResource.getTransactionLockType(currentTransaction);
                    Lock releaseLock  = new Lock(releaseName, releaseLockType, currentTransaction);
                    // Don't remove lock that was just added/updated
                    if (!releaseName.equals(name)) {
                        releaseResource.releaseLock(releaseLock);
                    }
                }
            }
            // If not compatible add to the queue, and prepare for block
            else {
                List<Lock> newReleaseLocks = new ArrayList<>();
                for (ResourceName releaseName : releaseLocks) {
                    ResourceEntry releaseResource = getResourceEntry(releaseName);
                    LockType releaseLockType = releaseResource.getTransactionLockType(currentTransaction);
                    Lock releaseLock  = new Lock(releaseName, releaseLockType, currentTransaction);
                    newReleaseLocks.add(releaseLock);
                }
                // should I add to front or back?
                resource.addToQueue(new LockRequest(transaction, newLock, newReleaseLocks), true);
                transaction.prepareBlock();
                shouldBlock = true;
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        synchronized (this) {
            Long currentTransaction = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, currentTransaction);
            ResourceEntry resource = getResourceEntry(name);
            LockType oldLockType = resource.getTransactionLockType(currentTransaction);

            // DuplicateLock error check
            if (!oldLockType.equals(LockType.NL)) {
                throw new DuplicateLockRequestException("Lock already held by transaction");
            }
            // If compatible and waitingQueue is empty then grant or update
            if (resource.waitingQueue.isEmpty() && resource.checkCompatible(lockType, currentTransaction)) {
                resource.grantOrUpdateLock(newLock);
            }
            // Else add to back of queue and prepare to block
            else {
                LockRequest newRequest = new LockRequest(transaction, newLock, new ArrayList<>());
                resource.addToQueue(newRequest, false);
                transaction.prepareBlock();
                shouldBlock = true;
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        synchronized (this) {
            Long currentTransaction = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            LockType oldLockType = resource.getTransactionLockType(currentTransaction);
            Lock lock = new Lock(name, oldLockType, currentTransaction);
            if (oldLockType.equals(LockType.NL)) {
                throw new NoLockHeldException("Release lock isn't held");
            }
            resource.releaseLock(lock);
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        synchronized (this) {
            Long currentTransaction = transaction.getTransNum();
            Lock newLock = new Lock(name, newLockType, currentTransaction);
            ResourceEntry resource = getResourceEntry(name);
            LockType oldLockType = resource.getTransactionLockType(currentTransaction);

            if (!LockType.substitutable(newLockType, oldLockType)) {
                throw new InvalidLockException("New Lock isn't a promotion");
            }

            if (oldLockType.equals(LockType.NL)) {
                throw new NoLockHeldException("No lock held");
            }

            if (oldLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("Lock of same type is already held");
            }

            if (resource.checkCompatible(newLockType, currentTransaction)) {
                resource.grantOrUpdateLock(newLock);
            } else {
                LockRequest newRequest = new LockRequest(transaction, newLock, new ArrayList<>());
                resource.addToQueue(newRequest, true);
                transaction.prepareBlock();
                shouldBlock = true;
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        List<Lock> locksOnResource = getLocks(name);
        for (Lock lock : locksOnResource) {
            if (lock.transactionNum == transaction.getTransNum()) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
