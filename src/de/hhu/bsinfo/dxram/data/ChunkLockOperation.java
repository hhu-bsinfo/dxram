package de.hhu.bsinfo.dxram.data;

/**
 * Flags to be used for locking/unlocking chunks and indicating the lock operation
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public enum ChunkLockOperation {
    /**
     * Do not execute any lock related operations.
     * Execute read lock/unlock.
     * Execute write lock/unlock.
     */
    NO_LOCK_OPERATION, READ_LOCK, WRITE_LOCK
}
