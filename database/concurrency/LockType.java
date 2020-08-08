package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // If either  a or b don't hold a lock then they are compatible
        if (a.equals(NL) || b.equals(NL)) {
            return true;
        }
        // Checks compatability if a is IS
        if (a.equals(IS)) {
            if (b.equals(X)) {
                return false;
            } else {
                return true;
            }
        }
        // Checks compatability if a is IX
        else if (a.equals(IX)) {
            if (b.equals(IS) || b.equals(IX)) {
                return true;
            } else {
                return false;
            }
        }
        // Checks compatability if a is S
        else if (a.equals(S)) {
            if (b.equals(IS) || b.equals(S)) {
                return true;
            } else {
                return false;
            }
        }
        // Checks compatability if a is SIX
        else if (a.equals(SIX)) {
            if (b.equals(IS)) {
                return true;
            } else {
                return false;
            }
        }
        // There is no compatability if a is X
        else {
            return false;
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        if (parentLockType.equals(NL) && childLockType.equals(NL)) {
            return true;
        }

        else if (parentLockType.equals(IS)) {
            if (childLockType.equals(S) || childLockType.equals(IS) || childLockType.equals(NL)) {
                return true;
            }
        }

        else if (parentLockType.equals(IX)) {
            return true;
        }

        else if (parentLockType.equals(SIX)) {
            if (childLockType.equals(S) || childLockType.equals(IS)) {
                return false;
            } else {
                return true;
            }
        }

        else if (parentLockType.equals(S)) {
            if (childLockType.equals(NL)) {
                return true;
            }
        }

        else if (parentLockType.equals(X)) {
            if (childLockType.equals(S) || childLockType.equals(NL)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // The required lock can obviously be substituted for a lock of the same type and covers NL case
        if (required.equals(substitute) || required.equals(NL)) {
            return true;
        }

        else if (required.equals(S)) {
            if (substitute.equals(X) || substitute.equals(SIX)) {
                return true;
            }
        }

        else if (required.equals(IS)) {
            if (substitute.equals(IX)) {
                return true;
            }
        }
        else if (required.equals(IX)) {
            if (substitute.equals(SIX)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

