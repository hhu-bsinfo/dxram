package de.hhu.bsinfo.dxram.log.storage.recovery;

import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.data.ChunkByteBuffer;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SegmentHeader;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.TemporaryVersionStorage;
import de.hhu.bsinfo.dxutils.hashtable.GenericHashTable;

/**
 * Recovery helper thread. Determines ChunkID ranges for to be recovered backup range and recovers segments as well.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
final class RecoveryHelperThread extends Thread {

    private RecoveryMetadata m_recoveryMetadata;

    private SecondaryLog m_secondaryLog;
    private DirectByteBufferWrapper m_wrapper;
    private TemporaryVersionStorage m_versionsForRecovery;
    private GenericHashTable<ChunkByteBuffer> m_largeChunks;
    private ReentrantLock m_largeChunkLock;
    private long m_lowestCID;
    private byte[] m_index;
    private ReentrantLock m_indexLock;
    private Recovery m_dxmemRecoveryOp;

    /**
     * Creates an instance of RecoveryHelperThread
     *
     * @param p_metadata
     *         the recovery metadata which is shared by all threads involved in the recovery
     * @param p_secondaryLog
     *         the secondary log
     * @param p_wrapper
     *         the byte buffer wrapper to load data into
     * @param p_versionsForRecovery
     *         the version data structures
     * @param p_largeChunks
     *         hash map to store large chunks in (chunks split to more than one log entry)
     * @param p_largeChunkLock
     *         a lock for accessing the hash map
     * @param p_lowestCID
     *         the lowest chunk ID stored in version array
     * @param p_index
     *         the segment index
     * @param p_indexLock
     *         lock to avoid recovering segments multiple times
     * @param p_dxmemRecoveryOp
     *         DXMem recovery operation to access the memory management during recovery
     */
    RecoveryHelperThread(final RecoveryMetadata p_metadata, final SecondaryLog p_secondaryLog,
            final DirectByteBufferWrapper p_wrapper, final TemporaryVersionStorage p_versionsForRecovery,
            final GenericHashTable<ChunkByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock,
            final long p_lowestCID, final byte[] p_index, final ReentrantLock p_indexLock,
            final Recovery p_dxmemRecoveryOp) {
        m_recoveryMetadata = p_metadata;

        m_secondaryLog = p_secondaryLog;
        m_wrapper = p_wrapper;
        m_versionsForRecovery = p_versionsForRecovery;
        m_largeChunks = p_largeChunks;
        m_largeChunkLock = p_largeChunkLock;
        m_lowestCID = p_lowestCID;
        m_index = p_index;
        m_indexLock = p_indexLock;
        m_dxmemRecoveryOp = p_dxmemRecoveryOp;
    }

    @Override
    public void run() {
        SegmentHeader[] segmentHeaders = m_secondaryLog.getSegmentHeaders();

        int idx = 0;
        while (true) {
            m_indexLock.lock();
            while (idx < m_index.length && m_index[idx] == 1) {
                idx++;
            }
            if (idx == m_index.length) {
                m_indexLock.unlock();
                break;
            }
            m_index[idx] = 1;
            m_indexLock.unlock();

            if (segmentHeaders[idx] != null && !segmentHeaders[idx].isEmpty()) {
                LogRecoveryHandler.recoverSegment(m_secondaryLog, idx, m_wrapper, m_versionsForRecovery, m_lowestCID,
                        m_recoveryMetadata, m_largeChunks, m_largeChunkLock, m_dxmemRecoveryOp);
            }
            idx++;
        }
    }
}
