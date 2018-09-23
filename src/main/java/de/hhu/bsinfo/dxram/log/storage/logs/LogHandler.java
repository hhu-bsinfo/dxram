/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.logs;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.ReorganizationThread;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxram.log.storage.writebuffer.BufferPool;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * For accessing the primary and secondary logs (buffers) from outside of this package.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public class LogHandler {

    private static final String PRIMLOG_PREFIX_FILENAME = "prim";
    private static final String SECLOG_PREFIX_FILENAME = "sec";
    private static final String VERLOG_PREFIX_FILENAME = "ver";
    private static final String POSTFIX_FILENAME = ".log";

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogHandler.class.getSimpleName());

    private final VersionHandler m_versionHandler;

    private final Scheduler m_scheduler;
    private final BackupRangeCatalog m_backupRangeCatalog;

    private final int m_logSegmentSize;

    private PrimaryLog m_primaryLog = null;
    private final WriterJobQueue m_writerJobQueue;
    private final WriterThread m_writerThread;
    private final ReorganizationThread m_reorgThread;

    private ReentrantReadWriteLock m_secondaryLogCreationLock;

    /**
     * Creates an instance of LogHandler.
     *
     * @param p_versionHandler
     *         the version handler accessed by the reorganization thread to gather all versions for a log and to
     *         create a version buffer when a new backup range is created
     * @param p_scheduler
     *         the scheduler to flush write buffer prior to transferring a recovered backup range and used by the
     *         secondary log to trigger the reorganization
     * @param p_backupRangeCatalog
     *         the backup range catalog for inserting/removing backup ranges, the primary log to flush all secondary log
     *         buffers, the writer thread to write to a secondary log, the reorganization thread to choose a log and
     *         the process thread to buffer data in secondary log buffer
     * @param p_bufferPool
     *         the buffer pool used by writer thread to return buffers
     * @param p_primaryLogSize
     *         the primary log size
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_logSegmentSize
     *         the log segment size
     * @param p_flashPageSize
     *         the flash page size
     * @param p_useChecksums
     *         whether to use checksums or not
     * @param p_utilizationActivateReorganization
     *         the threshold to trigger the reorganization
     * @param p_useTimestamps
     *         whether to use timestamps or not
     * @param p_coldDataThreshold
     *         the threshold for hot to cold data transformation
     * @param p_backupDirectory
     *         the backup directory
     * @param p_nodeID
     *         this node's node ID
     */
    public LogHandler(final VersionHandler p_versionHandler, final Scheduler p_scheduler,
            final BackupRangeCatalog p_backupRangeCatalog, final BufferPool p_bufferPool, final long p_primaryLogSize,
            final long p_secondaryLogSize, final int p_secondaryLogBufferSize, final int p_logSegmentSize,
            final int p_flashPageSize, final boolean p_useChecksums, final int p_utilizationActivateReorganization,
            final boolean p_useTimestamps, final int p_coldDataThreshold, final String p_backupDirectory,
            final short p_nodeID) {
        m_versionHandler = p_versionHandler;

        m_scheduler = p_scheduler;
        m_backupRangeCatalog = p_backupRangeCatalog;

        m_logSegmentSize = p_logSegmentSize;

        m_secondaryLogCreationLock = new ReentrantReadWriteLock(false);

        if (p_secondaryLogBufferSize == 0 || !LogComponent.TWO_LEVEL_LOGGING_ACTIVATED) {

            LOGGER.info("Two-level logging is disabled. Performance might be impaired!");
        } else {
            // Create primary log
            try {
                String primaryLogFileName = p_backupDirectory + PRIMLOG_PREFIX_FILENAME + 'N' + p_nodeID + '_' +
                        (p_useChecksums ? "1" : "0") + '_' + (p_useTimestamps ? "1" : "0") + '_' + POSTFIX_FILENAME;
                m_primaryLog =
                        new PrimaryLog(m_backupRangeCatalog, primaryLogFileName, p_primaryLogSize, p_flashPageSize);
            } catch (final IOException e) {

                LOGGER.error("Primary log creation failed", e);

            }

            LOGGER.trace("Initialized primary log (%d)", p_primaryLogSize);

        }

        m_writerJobQueue = new WriterJobQueue();

        m_writerThread = new WriterThread(m_primaryLog, m_backupRangeCatalog, m_writerJobQueue, p_bufferPool);
        m_writerThread.setName("Logging: Writer Thread");
        m_writerThread.start();

        m_reorgThread =
                new ReorganizationThread(p_versionHandler, m_backupRangeCatalog, p_secondaryLogSize, p_logSegmentSize,
                        p_utilizationActivateReorganization, p_coldDataThreshold, p_useTimestamps);
        m_reorgThread.setName("Logging: Reorganization Thread");
        m_reorgThread.start();
    }

    /**
     * Closes all log components (not the logs themselves).
     */
    public void close() {
        if (m_primaryLog != null) {
            try {
                m_primaryLog.close();
            } catch (final IOException ignored) {
                LOGGER.warn("Could not close primary log!");
            }
        }

        m_writerThread.shutdown();
        try {
            m_writerThread.join();

            LOGGER.info("Shutdown of WriterThread successful");
        } catch (final InterruptedException e) {
            LOGGER.warn("Could not wait for writer thread to finish. Interrupted.", e);
        }

        m_reorgThread.interrupt();
        m_reorgThread.shutdown();
        try {
            m_reorgThread.join();

            LOGGER.info("Shutdown of ReorganizationThread successful");
        } catch (final InterruptedException e) {
            LOGGER.warn("Could not wait for reorganization thread to finish. Interrupted.", e);
        }

        // Secondary logs, secondary log buffers, version buffers and version logs are closed via backup range
        // catalog
    }

    /**
     * Returns the write capacity for large, chained log entries.
     *
     * @return the write capacity
     */
    public int getWriteCapacity() {
        return m_logSegmentSize * WriterJobQueue.getCapacity();
    }

    /**
     * Creates a new backup range.
     *
     * @param p_rangeID
     *         the range ID
     * @param p_owner
     *         the owner
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the log segment size
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_flashPageSize
     *         the flash page size
     * @param p_utilizationPromptReorganization
     *         the threshold to trigger the reorganization
     * @param p_useChecksums
     *         whether to use checksums or not
     * @param p_useTimestamps
     *         whether to use timestamps or not
     * @param p_initTime
     *         the initialization time or 0 if timestamps are disabled
     * @param p_backupDirectory
     *         the backup directory
     * @return whether the backup range was created or not
     */
    public boolean createBackupRange(final short p_rangeID, final short p_owner, final long p_secondaryLogSize,
            final int p_logSegmentSize, final int p_secondaryLogBufferSize, final int p_flashPageSize,
            final int p_utilizationPromptReorganization, final boolean p_useChecksums, final boolean p_useTimestamps,
            final long p_initTime, final String p_backupDirectory) {
        boolean ret = true;

        // Initialize a new backup range created by p_owner
        m_secondaryLogCreationLock.writeLock().lock();

        if (!m_backupRangeCatalog.exists(p_owner, p_rangeID)) {
            String secLogFileName =
                    p_backupDirectory + 'N' + NodeID.toHexString(p_owner) + '_' + SECLOG_PREFIX_FILENAME +
                            NodeID.toHexString(p_owner) + '_' + p_rangeID + '_' + (p_useChecksums ? "1" : "0") + '_' +
                            (p_useTimestamps ? "1" : "0") + POSTFIX_FILENAME;
            String verLogFileName =
                    p_backupDirectory + 'N' + NodeID.toHexString(p_owner) + '_' + VERLOG_PREFIX_FILENAME +
                            NodeID.toHexString(p_owner) + '_' + p_rangeID + POSTFIX_FILENAME;
            try {
                VersionBuffer versionBuffer = m_versionHandler.createVersionBuffer(p_owner, verLogFileName);
                SecondaryLogBuffer secLogBuffer =
                        new SecondaryLogBuffer(m_scheduler, versionBuffer, p_owner, p_owner, p_rangeID,
                                p_secondaryLogBufferSize, p_secondaryLogSize, p_flashPageSize, p_logSegmentSize,
                                p_utilizationPromptReorganization, p_useTimestamps, p_initTime, secLogFileName);
                m_backupRangeCatalog.insertRange(p_owner, p_rangeID, secLogBuffer, versionBuffer);
            } catch (final IOException e) {

                LOGGER.error("Initialization of backup range %d failed: %s", p_rangeID, e);

                ret = false;
            }
        }

        m_secondaryLogCreationLock.writeLock().unlock();

        return ret;
    }

    /**
     * Creates or transfers a recovered backup range.
     *
     * @param p_rangeID
     *         the range ID
     * @param p_owner
     *         the owner
     * @param p_originalRangeID
     *         the range ID prior to the reorganization
     * @param p_originalOwner
     *         the owner prior to the reorganization
     * @param p_isNewBackupRange
     *         whether this is a new backup range or an old one needs to be transferred
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the log segment size
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_flashPageSize
     *         the flash page size
     * @param p_utilizationPromptReorganization
     *         the threshold to trigger the reorganization
     * @param p_useChecksums
     *         whether to use checksums or not
     * @param p_useTimestamps
     *         whether to use timestamps or not
     * @param p_initTime
     *         the initialization time or 0 if timestamps are disabled
     * @param p_backupDirectory
     *         the backup directory
     * @return whether the backup range was created or not
     */
    public boolean createRecoveredBackupRange(final short p_rangeID, final short p_owner, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewBackupRange, final long p_secondaryLogSize,
            final int p_logSegmentSize, final int p_secondaryLogBufferSize, final int p_flashPageSize,
            final int p_utilizationPromptReorganization, final boolean p_useChecksums, final boolean p_useTimestamps,
            final long p_initTime, final String p_backupDirectory) {
        boolean ret = true;

        // Flush write buffer to have all data in secondary log (buffer) for given range (if this peer stored backups
        // for this range before). There is no condition to check here. There should not be any relevant data in write
        // buffer anyways as the crashed peer is long gone.
        m_scheduler.flushWriteBuffer();

        m_secondaryLogCreationLock.writeLock().lock();

        String secLogFileName =
                p_backupDirectory + 'N' + NodeID.toHexString(p_originalOwner) + '_' + SECLOG_PREFIX_FILENAME +
                        NodeID.toHexString(p_owner) + '_' + p_rangeID + '_' + (p_useChecksums ? "1" : "0") + '_' +
                        (p_useTimestamps ? "1" : "0") + POSTFIX_FILENAME;
        String verLogFileName =
                p_backupDirectory + 'N' + NodeID.toHexString(p_originalOwner) + '_' + VERLOG_PREFIX_FILENAME +
                        NodeID.toHexString(p_owner) + '_' + p_rangeID + POSTFIX_FILENAME;

        if (p_isNewBackupRange) {
            // This is a new backup peer determined during recovery (replicas will be sent shortly by p_owner)
            if (!m_backupRangeCatalog.exists(p_owner, p_rangeID)) {

                try {
                    VersionBuffer versionBuffer = m_versionHandler.createVersionBuffer(p_owner, verLogFileName);
                    SecondaryLogBuffer secLogBuffer =
                            new SecondaryLogBuffer(m_scheduler, versionBuffer, p_owner, p_originalOwner, p_rangeID,
                                    p_secondaryLogBufferSize, p_secondaryLogSize, p_flashPageSize, p_logSegmentSize,
                                    p_utilizationPromptReorganization, p_useTimestamps, p_initTime, secLogFileName);
                    m_backupRangeCatalog.insertRange(p_owner, p_rangeID, secLogBuffer, versionBuffer);
                } catch (final IOException e) {

                    LOGGER.error("Transfer of backup range %d from 0x%X to 0x%X failed! %s", p_originalRangeID,
                            p_originalOwner, p_owner, e);

                    ret = false;
                }
            } else {
                LOGGER.warn("Transfer of backup range %d from 0x%X to 0x%X failed! Backup range already exists!",
                        p_originalRangeID, p_originalOwner, p_owner);
            }
        } else {
            // Transfer recovered backup range from p_originalOwner to p_owner
            SecondaryLogBuffer secLogBuffer =
                    m_backupRangeCatalog.getSecondaryLogBuffer(p_originalOwner, p_originalRangeID);
            VersionBuffer versionBuffer = m_backupRangeCatalog.getVersionBuffer(p_originalOwner, p_originalRangeID);
            if (secLogBuffer != null && versionBuffer != null) {
                // This is an old backup peer (it has the entire data already)
                try {
                    secLogBuffer.transferBackupRange(p_owner, p_rangeID, secLogFileName);
                    versionBuffer.transferBackupRange(verLogFileName);
                } catch (final IOException e) {
                    LOGGER.error("Log file could not be renamed.", e);
                }

                m_backupRangeCatalog
                        .moveRange(p_originalOwner, p_originalRangeID, p_owner, p_rangeID, secLogBuffer, versionBuffer);
            } else {

                LOGGER.warn("Transfer of backup range %d from 0x%X to 0x%X failed! Secondary log already exists!",
                        p_originalRangeID, p_originalOwner, p_owner);

            }
        }

        m_secondaryLogCreationLock.writeLock().unlock();

        return ret;
    }

    /**
     * Removes a backup range.
     *
     * @param p_owner
     *         the owner
     * @param p_rangeID
     *         the range ID
     */
    public void removeBackupRange(final short p_owner, final short p_rangeID) {
        m_secondaryLogCreationLock.writeLock().lock();
        try {
            m_backupRangeCatalog.removeAndCloseBuffersAndLogs(p_owner, p_rangeID);
        } catch (final IOException e) {
            LOGGER.trace("Backup range could not be removed from hard drive.", e);
        }
        m_secondaryLogCreationLock.writeLock().unlock();
    }

    /**
     * Buffers data in corresponding secondary log buffer.
     *
     * @param p_buffer
     *         the buffer containing the data
     * @param p_logEntrySize
     *         the write size
     * @param p_rangeID
     *         the range ID
     * @param p_owner
     *         the owner
     * @return a buffer for flushing or null if data was appended to buffer
     */
    public DirectByteBufferWrapper bufferDataForSecondaryLog(final DirectByteBufferWrapper p_buffer,
            final int p_logEntrySize, final short p_rangeID, final short p_owner) {
        SecondaryLogBuffer secLogBuffer = m_backupRangeCatalog.getSecondaryLogBuffer(p_owner, p_rangeID);

        if (secLogBuffer == null) {
            LOGGER.error("Cannot buffer data as backup range is not available");
            return null;
        }

        return secLogBuffer.bufferData(p_buffer, p_logEntrySize);
    }

    /**
     * Writes to secondary log by posting a new job for the writer thread.
     *
     * @param p_buffer
     *         the buffer containing the data
     * @param p_logEntrySize
     *         the write size
     * @param p_combinedRangeID
     *         the combined range ID
     * @param p_returnBuffer
     *         whether to return the write buffer to buffer pool or not
     */
    public void writeToSecondaryLog(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize,
            final int p_combinedRangeID, final boolean p_returnBuffer) {
        if (p_returnBuffer) {
            m_writerJobQueue
                    .pushJob(WriterJobQueue.JobID.SEC_LOG_RETURN_BUFFER, p_buffer, p_logEntrySize, p_combinedRangeID);
        } else {
            m_writerJobQueue.pushJob(WriterJobQueue.JobID.SEC_LOG, p_buffer, p_logEntrySize, p_combinedRangeID);
        }
    }

    /**
     * Writes to primary log by posting a new job for the writer thread.
     *
     * @param p_buffer
     *         the buffer containing the data
     * @param p_logEntrySize
     *         the write size
     */
    public void writeToPrimaryLog(final DirectByteBufferWrapper p_buffer, final int p_logEntrySize) {
        m_writerJobQueue.pushJob(WriterJobQueue.JobID.PRIM_LOG, p_buffer, p_logEntrySize, -1);
    }

    /**
     * Wakes up the reorganization thread.
     *
     * @param p_secondaryLog
     *         the secondary log to reorganize
     */
    public void signalReorganization(final SecondaryLog p_secondaryLog) {
        m_reorgThread.setLogToReorgImmediately(p_secondaryLog, false);
    }

    /**
     * Wakes up the reorganization thread and waits until reorganization is finished.
     *
     * @param p_secondaryLog
     *         the secondary log to reorganize
     */
    public void signalReorganizationBlocking(final SecondaryLog p_secondaryLog) {
        m_reorgThread.setLogToReorgImmediately(p_secondaryLog, true);
    }

    /**
     * Grants access to a log to be reorganized by the reorganization thread.
     */
    public void grantAccessToReorganization() {
        m_reorgThread.grantAccessToCurrentLog();
    }

    /**
     * Blocks the reorganization thread.
     */
    public void blockReorganizationThread() {
        m_reorgThread.block();
    }

    /**
     * Unblocks the reorganization thread.
     */
    public void unblockReorganizationThread() {
        m_reorgThread.unblock();
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization() {
        StringBuilder ret;
        long allBytesAllocated = 0;
        long allBytesOccupied = 0;
        long counterAllocated;
        long counterOccupied;
        long occupiedInRange;
        SecondaryLogBuffer[] secLogBuffers;
        VersionBuffer[] versionBuffers;

        ret = new StringBuilder(
                "***********************************************************************\n" + "*Primary log: " +
                        m_primaryLog.getOccupiedSpace() + " bytes\n" +
                        "***********************************************************************\n\n" +
                        "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" +
                        "+Secondary logs:\n");

        for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
            secLogBuffers = m_backupRangeCatalog.getAllSecondaryLogBuffers((short) i);
            if (secLogBuffers != null) {
                versionBuffers = m_backupRangeCatalog.getAllVersionBuffers((short) i);
                counterAllocated = 0;
                counterOccupied = 0;
                ret.append("++Node ").append(NodeID.toHexString((short) i)).append(":\n");
                for (int j = 0; j < secLogBuffers.length; j++) {
                    if (secLogBuffers[j] != null) {
                        ret.append("+++Backup range ").append(j).append(": ");
                        if (secLogBuffers[j].getLog().isAccessed()) {
                            ret.append("#Active log# ");
                        }

                        counterAllocated +=
                                secLogBuffers[j].getLog().getFileSize() + versionBuffers[j].getLogFileSize();
                        occupiedInRange = secLogBuffers[j].getOccupiedSpace();
                        counterOccupied += occupiedInRange;

                        ret.append(occupiedInRange).append(" bytes (in buffer: ")
                                .append(secLogBuffers[j].getOccupiedSpace()).append(" bytes)\n");
                        ret.append(secLogBuffers[j].getLog().getSegmentDistribution()).append('\n');
                    }
                }
                ret.append("++Bytes per node: allocated -> ").append(counterAllocated).append(", occupied -> ")
                        .append(counterOccupied).append('\n');
                allBytesAllocated += counterAllocated;
                allBytesOccupied += counterOccupied;
            }
        }
        ret.append("Complete size: allocated -> ").append(allBytesAllocated).append(", occupied -> ")
                .append(allBytesOccupied).append('\n');
        ret.append("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

        return ret.toString();
    }

}
