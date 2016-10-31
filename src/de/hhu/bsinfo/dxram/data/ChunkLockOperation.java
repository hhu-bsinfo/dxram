
package de.hhu.bsinfo.dxram.data;

/**
 * Flags to be used for locking/unlocking chunks and indicating the lock operation
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public enum ChunkLockOperation {
    /**
     * Do not execute any lock related operations.
     */
    NO_LOCK_OPERATION,
    /**
     * Execute read lock/unlock.
     */
    READ_LOCK,
    /**
     * Execute write lock/unlock.
     */
    WRITE_LOCK
}
