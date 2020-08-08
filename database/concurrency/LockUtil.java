package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        LockContext par = lockContext.parent;
        if (par != null && par.getAutoEscalate() && par.saturation(transaction) >= 0.20 && par.capacity() >= 10) {
            lockContext.parent.escalate(transaction);
        }
        LockType currentLockType = lockContext.getExplicitLockType(transaction);
        LockType effectiveType = lockContext.getEffectiveLockType(transaction);
        if (LockType.substitutable(effectiveType, lockType)) {
            return;
        }
        List<ResourceName> releaseLocks = new ArrayList<>();
        releaseLocks.add(lockContext.name);
        if (lockType.equals(LockType.S)) {
            // Parent lock type can't be S, X or SIX
            if (currentLockType.equals(LockType.IX)) {
                ensureProperAncestors(lockContext, LockType.X, transaction);
                lockContext.promote(transaction, LockType.SIX);
            } else {
                ensureProperAncestors(lockContext, lockType, transaction);
                if (currentLockType.equals(LockType.NL)) {
                    lockContext.acquire(transaction, lockType);
                } else if (currentLockType.equals(LockType.IS)) {
                    lockContext.escalate(transaction);
                }
            }
            // Parent lock type can't be X
        } else {
            ensureProperAncestors(lockContext, lockType, transaction);
            if (currentLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, lockType);
            } else if (currentLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
                lockContext.promote(transaction, lockType);
            } else if (currentLockType.equals(LockType.IX) || currentLockType.equals(LockType.S)) {
                lockContext.promote(transaction, lockType.X);
            } else {
                lockContext.escalate(transaction);
            }
        }
    }

    public static void ensureProperAncestors(LockContext lockContext, LockType lockType,
                                             TransactionContext transaction) {
        if (lockContext.parent == null) {
            return;
        }
        LockContext pointer = lockContext.parent;
        Deque<LockContext> parentStack = new ArrayDeque<>();
        while (pointer != null) {
            LockType parType = pointer.getExplicitLockType(transaction);
            if (LockType.canBeParentLock(parType, lockType)) {
                break;
            }
            parentStack.addFirst(pointer);
            pointer = pointer.parent;
        }
        while (!parentStack.isEmpty()) {
            LockContext parContext = parentStack.poll();
            updateParent(parContext, lockType, transaction);
        }
    }

    public static void updateParent(LockContext lockContext, LockType lockType,
                                      TransactionContext transaction) {
        LockType parType = lockContext.getExplicitLockType(transaction);
        List<ResourceName> releaseLock = new ArrayList<>();
        releaseLock.add(lockContext.getResourceName());
        if (lockType.equals(LockType.X)) {
            if (parType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.IX);
            } else if (parType.equals(LockType.S)) {
                lockContext.promote(transaction, LockType.SIX);
            } else {
                lockContext.promote(transaction, LockType.IX);
            }
        } else {
            if (parType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.IS);
            }
        }
    }

}
