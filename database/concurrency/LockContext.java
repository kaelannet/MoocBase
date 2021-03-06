package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    // Where or not auto escalation is enabled or disabled
    protected boolean autoEscalation;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
        this.autoEscalation = true;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        LockType explicitLockType = getExplicitLockType(transaction);

        // Read-only error check
        if (readonly) {
            throw new UnsupportedOperationException("Context is read-only");
        }
        if (parent != null) {
            LockType parentType = parent.getExplicitLockType(transaction);
            if (LockType.canBeParentLock(parentType, lockType)) {
                lockman.acquire(transaction, getResourceName(), lockType);
                Long transNum = transaction.getTransNum();
                parent.numChildLocks.put(transNum, parent.numChildLocks.getOrDefault(transNum, 0) + 1);
                return;
            } else {
                throw new InvalidLockException("Request is invalid");
            }
        }
        lockman.acquire(transaction, getResourceName(), lockType);
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {

        //Error checking
        if (readonly) {
            throw new UnsupportedOperationException("Resource is readonly");
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on resource by transaction");
        }
        int numDescendents = getDescendants(transaction).size();
        if (numDescendents != 0) {
            throw new InvalidLockException("There exists a descendant with a lock");
        } else {
            lockman.release(transaction, getResourceName());
            Long transNum = transaction.getTransNum();
            if (parent != null) {
                parent.numChildLocks.put(transNum, parent.numChildLocks.getOrDefault(transNum, 0) - 1);
            }
        }
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {

        //Error checking
        if (readonly) {
            throw new UnsupportedOperationException("Resource is readonly");
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on resource by transaction");
        }
        if (type.equals(newLockType)) {
            throw new DuplicateLockRequestException("LockType requested is a duplicate of current lock");
        }
        if (!LockType.substitutable(newLockType, type)) {
            throw new InvalidLockException("LockType requested is not a promotion");
        }

        if (parent != null) {
            LockType parentType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentType, newLockType)) {
                throw new InvalidLockException("LockType breaks multigranularity");
            }
        }
        if (newLockType.equals(LockType.SIX)) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("SIX ancestor exists so promotion is redundant");
            } else {
                List<ResourceName> releaseLocks = sisDescendants(transaction);
                Long transactionNum = transaction.getTransNum();
                for (ResourceName resource : releaseLocks) {
                    LockContext context = LockContext.fromResourceName(lockman, resource);
                    int currNum = context.parent.numChildLocks.getOrDefault(transactionNum, 0);
                    context.parent.numChildLocks.put(transactionNum, currNum - 1);
                }
                releaseLocks.add(getResourceName());
                lockman.acquireAndRelease(transaction, getResourceName(), newLockType, releaseLocks);
                return;
            }
        }
        lockman.promote(transaction, getResourceName(), newLockType);
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {

        if (readonly) {
            throw new UnsupportedOperationException("Resource is readonly");
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on resource");
        }
        if (type.equals(LockType.S) || type.equals(LockType.X)) {
            return;
        }
        List<ResourceName> releaseLocks = getDescendants(transaction);
        for(ResourceName resource : releaseLocks) {
            LockContext context = LockContext.fromResourceName(lockman, resource);
            int currNum = context.parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
            context.parent.numChildLocks.put(transaction.getTransNum(), currNum - 1);
        }
        releaseLocks.add(getResourceName());
        if (type.equals(LockType.IS)) {
            lockman.acquireAndRelease(transaction, getResourceName(), LockType.S, releaseLocks);
        } else {
            lockman.acquireAndRelease(transaction, getResourceName(), LockType.X, releaseLocks);
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        LockType lockType = getExplicitLockType(transaction);
        if (lockType.equals(LockType.NL) && parent != null) {
            if (hasSIXAncestor(transaction)) {
                return LockType.S;
            }
            lockType = parent.getEffectiveLockType(transaction);
            if (lockType.equals(LockType.IS) || lockType.equals(LockType.IX)) {
                return LockType.NL;
            }
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext pointer = parent;
        while(pointer != null) {
            LockType lockType = pointer.lockman.getLockType(transaction, pointer.getResourceName());
            if (lockType.equals(LockType.SIX)) {
                return true;
            }
            pointer = pointer.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> descendants = getDescendants(transaction);
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (ResourceName descendant : descendants) {
            LockType des = LockContext.fromResourceName(lockman, descendant).getExplicitLockType(transaction);
            if (des.equals(LockType.IS) || des.equals(LockType.S)) {
                sisDescendants.add(descendant);
            }
        }
        return sisDescendants;
    }


    /**
     * Helper method to get a list of resourceNames of all locks that are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> getDescendants(TransactionContext transaction) {
        List<ResourceName> descendants = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            ResourceName name = lock.name;
            LockType type = lock.lockType;
            if (name.isDescendantOf(getResourceName())) {
                descendants.add(name);
            }
        }
        return descendants;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        LockType lockType = lockman.getLockType(transaction, getResourceName());
        return lockType;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    public boolean getAutoEscalate() {
        return this.autoEscalation;
    }

    public void setAutoEscalate(boolean bool) {
        this.autoEscalation = bool;
    }
}

