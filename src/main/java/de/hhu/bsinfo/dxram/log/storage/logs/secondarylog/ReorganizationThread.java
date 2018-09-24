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

package de.hhu.bsinfo.dxram.log.storage.logs.secondarylog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.TemporaryVersionStorage;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.Version;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Reorganization thread.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.06.2014
 */
public final class ReorganizationThread extends Thread {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ReorganizationThread.class.getSimpleName());

    private static final TimePool SOP_REORG_GET_VERSIONS = new TimePool(ReorganizationThread.class, "GetVersions");
    private static final TimePool SOP_REORG_READ_SEGMENT = new TimePool(ReorganizationThread.class, "ReadSegment");
    private static final TimePool SOP_REORG_PROCESS_SEGMENT =
            new TimePool(ReorganizationThread.class, "ProcessSegment");
    private static final TimePool SOP_REORG_WRITE_SEGMENT = new TimePool(ReorganizationThread.class, "WriteSegment");
    private static final ValuePool SOP_REORG_BYTES_FREED = new ValuePool(ReorganizationThread.class, "BytesFreed");

    static {
        StatisticsManager.get().registerOperation(ReorganizationThread.class, SOP_REORG_GET_VERSIONS);
        StatisticsManager.get().registerOperation(ReorganizationThread.class, SOP_REORG_READ_SEGMENT);
        StatisticsManager.get().registerOperation(ReorganizationThread.class, SOP_REORG_PROCESS_SEGMENT);
        StatisticsManager.get().registerOperation(ReorganizationThread.class, SOP_REORG_WRITE_SEGMENT);
        StatisticsManager.get().registerOperation(ReorganizationThread.class, SOP_REORG_BYTES_FREED);
    }

    private final BackupRangeCatalog m_backupRangeCatalog;
    private final VersionHandler m_versionHandler;

    private final LinkedHashSet<SecondaryLog> m_reorganizationRequests;
    private final TemporaryVersionStorage m_allVersions;

    private final ReentrantLock m_reorganizationLock;
    private final Condition m_reorganizationFinishedCondition;
    private final ReentrantLock m_requestLock;
    private final ReentrantLock m_recoveryLock;

    private final DirectByteBufferWrapper m_reorgSegmentData;

    private final long m_secondaryLogSize;
    private final int m_logSegmentSize;
    private final int m_activateReorganizationThreshold;
    private final int m_coldDataThreshold;
    private final boolean m_useTimestamps;
    private final int m_iterationsPerLog;

    private byte m_counter;
    private int m_segmentReorgCounter;

    private volatile SecondaryLog m_secLog;
    private volatile boolean m_reorgThreadWaits;
    private volatile boolean m_accessGrantedForReorgThread;
    private volatile boolean m_shutdown;

    /**
     * Creates an instance of ReorganizationThread.
     *
     * @param p_backupRangeCatalog
     *         the backup range catalog
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_utilizationActivateReorganization
     *         the threshold to consider a log for reorganization
     */
    public ReorganizationThread(final VersionHandler p_versionHandler, final BackupRangeCatalog p_backupRangeCatalog,
            final long p_secondaryLogSize, final int p_logSegmentSize, final int p_utilizationActivateReorganization,
            final int p_coldDataThreshold, final boolean p_useTimestamps) {
        m_backupRangeCatalog = p_backupRangeCatalog;
        m_versionHandler = p_versionHandler;

        m_secondaryLogSize = p_secondaryLogSize;
        m_logSegmentSize = p_logSegmentSize;
        m_iterationsPerLog = (int) (p_secondaryLogSize / p_logSegmentSize * 0.33f);
        m_activateReorganizationThreshold =
                (int) ((float) p_utilizationActivateReorganization / 100 * m_secondaryLogSize);
        m_coldDataThreshold = p_coldDataThreshold;
        m_useTimestamps = p_useTimestamps;

        m_allVersions = new TemporaryVersionStorage(m_secondaryLogSize);

        m_reorganizationLock = new ReentrantLock(false);
        m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();

        m_reorganizationRequests = new LinkedHashSet<SecondaryLog>();
        m_requestLock = new ReentrantLock(false);

        m_recoveryLock = new ReentrantLock(false);

        m_reorgSegmentData = new DirectByteBufferWrapper(p_logSegmentSize, true);

        m_counter = 0;
    }

    /**
     * Get access to secondary log for reorganization thread.
     *
     * @param p_secLog
     *         the Secondary Log
     */
    private static void leaveSecLog(final SecondaryLog p_secLog) {
        if (p_secLog.isAccessed()) {
            p_secLog.setAccessFlag(false);
        }
    }

    /**
     * Shutdown.
     */
    public void shutdown() {
        m_shutdown = true;
    }

    /**
     * Block the reorganization thread.
     * Is called during recovery.
     */
    public void block() {
        while (!m_recoveryLock.tryLock()) {
            interrupt();
        }
    }

    /**
     * Unblock the reorganization thread.
     * Is called during recovery.
     */
    public void unblock() {
        m_recoveryLock.unlock();
    }

    @Override
    public void run() {
        int counter = 0;
        long lowestLID = 0;
        SecondaryLog secondaryLog = null;

        while (!m_shutdown) {
            m_recoveryLock.lock();
            m_reorganizationLock.lock();
            // Check if there is an urgent reorganization request -> reorganize complete secondary log and signal
            if (m_secLog != null) {
                // Leave current secondary log
                counter = leaveSecondaryLog(secondaryLog, counter);

                // Process urgent request
                processUrgentRequest();
                m_recoveryLock.unlock();
                continue;
            }
            m_reorganizationLock.unlock();

            // Check if there are normal reorganization requests -> reorganize complete secondary logs
            m_requestLock.lock();
            if (!m_reorganizationRequests.isEmpty()) {
                m_requestLock.unlock();
                // Leave current secondary log
                counter = leaveSecondaryLog(secondaryLog, counter);

                // Process all reorganization requests
                processLowPriorityRequest();
                m_recoveryLock.unlock();
                continue;
            }
            m_requestLock.unlock();

            if (counter == 0) {
                // This is the first iteration -> choose secondary log and gather versions
                secondaryLog = chooseLog();
                if (secondaryLog != null && (secondaryLog.getOccupiedSpace() > m_activateReorganizationThreshold ||
                        secondaryLog.needToBeReorganized())) {
                    getAccessToSecLog(secondaryLog);
                    if (!interrupted()) {

                        SOP_REORG_GET_VERSIONS.start();

                        try {
                            lowestLID = m_versionHandler
                                    .getCurrentVersions(secondaryLog.getOwner(), secondaryLog.getRangeID(),
                                            m_allVersions, true);
                        } catch (IOException e) {
                            LOGGER.error(e);
                            SOP_REORG_GET_VERSIONS.stop();
                            m_recoveryLock.unlock();
                            continue;
                        }

                        SOP_REORG_GET_VERSIONS.stop();

                        if (interrupted()) {
                            m_recoveryLock.unlock();
                            continue;
                        }

                        // TODO: flush primary and secondary log buffers
                    } else {
                        m_recoveryLock.unlock();
                        continue;
                    }
                } else {
                    // Nothing to do -> wait for a while to reduce cpu load
                    m_recoveryLock.unlock();
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignored) {
                    }
                    continue;
                }
            }

            // Reorganize one segment
            if (secondaryLog != null) {

                LOGGER.trace("Going to reorganize %s", secondaryLog.getRangeID());

                getAccessToSecLog(secondaryLog);

                if (!interrupted()) {
                    final long start = System.currentTimeMillis();
                    if (!reorganizeIteratively(secondaryLog, m_reorgSegmentData, m_allVersions, lowestLID)) {
                        // Reorganization failed -> switch log
                        counter = m_iterationsPerLog;
                    }

                    if (!interrupted()) {

                        LOGGER.trace("Time to reorganize segment: %d", System.currentTimeMillis() - start);

                    } else {

                        LOGGER.debug("Reorganization of segment was interrupted! Time: %d",
                                System.currentTimeMillis() - start);

                    }

                    if (counter++ == m_iterationsPerLog || !secondaryLog.needToBeReorganized() &&
                            secondaryLog.getOccupiedSpace() < m_activateReorganizationThreshold) {
                        // This was the last iteration for current secondary log or
                        // further reorganization not necessary -> clean-up
                        counter = leaveSecondaryLog(secondaryLog, counter);
                    }
                }
            }
            m_recoveryLock.unlock();
        }
    }

    /**
     * Process urgent request by reorganizing the entire secondary log.
     *
     * @lock m_reorganizationLock must be acquired
     */
    private void processUrgentRequest() {
        // Reorganize complete secondary log
        SecondaryLog secondaryLog = m_secLog;

        LOGGER.debug("Got urgent reorganization request for %s", m_secLog);

        m_reorganizationLock.unlock();
        getAccessToSecLog(secondaryLog);

        if (!interrupted()) {
            m_reorganizationLock.lock();

            SOP_REORG_GET_VERSIONS.start();

            long lowestLID;
            try {
                lowestLID = m_versionHandler
                        .getCurrentVersions(secondaryLog.getOwner(), secondaryLog.getRangeID(), m_allVersions, true);
            } catch (IOException e) {
                LOGGER.error(e);
                SOP_REORG_GET_VERSIONS.stop();
                m_reorganizationLock.unlock();
                return;
            }

            SOP_REORG_GET_VERSIONS.stop();

            if (!interrupted()) {

                // TODO: flush primary and secondary log buffers

                reorganizeAll(secondaryLog, m_reorgSegmentData, m_allVersions, lowestLID);
                secondaryLog.resetReorgSegment();
                leaveSecLog(secondaryLog);
                m_allVersions.clear();

                if (!interrupted()) {
                    m_secLog = null;
                    m_reorganizationFinishedCondition.signalAll();
                }
            } else {
                secondaryLog.resetReorgSegment();
                leaveSecLog(secondaryLog);
                m_allVersions.clear();
            }
            m_reorganizationLock.unlock();
        }
    }

    /**
     * Process low priority requests by reorganizing the entire secondary log.
     *
     * @lock m_reorganizationLock must be acquired
     */
    private void processLowPriorityRequest() {
        long lowestLID;
        SecondaryLog secondaryLog;
        Iterator<SecondaryLog> iter;

        while (!interrupted() && !m_reorganizationRequests.isEmpty() && !m_shutdown) {
            m_reorganizationLock.lock();
            if (m_secLog != null) {
                // Favor urgent request
                m_reorganizationLock.unlock();
                break;
            }
            m_reorganizationLock.unlock();

            m_requestLock.lock();
            iter = m_reorganizationRequests.iterator();
            secondaryLog = iter.next();
            iter.remove();

            LOGGER.debug("Got reorganization request for %s. Queue length: %d", secondaryLog.getRangeID(),
                    m_reorganizationRequests.size());

            m_requestLock.unlock();

            long start = System.currentTimeMillis();
            // Reorganize complete secondary log
            getAccessToSecLog(secondaryLog);
            if (!interrupted()) {

                SOP_REORG_GET_VERSIONS.start();

                try {
                    lowestLID = m_versionHandler
                            .getCurrentVersions(secondaryLog.getOwner(), secondaryLog.getRangeID(), m_allVersions,
                                    true);
                } catch (IOException e) {
                    LOGGER.error(e);
                    SOP_REORG_GET_VERSIONS.stop();

                    secondaryLog.resetReorgSegment();
                    leaveSecLog(secondaryLog);
                    break;
                }

                SOP_REORG_GET_VERSIONS.stop();

                if (!interrupted()) {

                    // TODO: flush primary and secondary log buffers

                    int counter = 0;
                    while (secondaryLog.getOccupiedSpace() > m_activateReorganizationThreshold ||
                            secondaryLog.needToBeReorganized()) {
                        // Reorganize if any updates arrived, only
                        reorganizeIteratively(secondaryLog, m_reorgSegmentData, m_allVersions, lowestLID);
                        if (++counter == m_iterationsPerLog) {
                            break;
                        }
                    }
                }
            }
            secondaryLog.resetReorgSegment();
            leaveSecLog(secondaryLog);
            m_allVersions.clear();

            LOGGER.trace("Time to reorganize complete log: %d", System.currentTimeMillis() - start);

        }
    }

    /**
     * Reset data structures and leave secondary log.
     *
     * @param p_secondaryLog
     *         the current secondary log
     * @param p_counter
     *         the current iteration
     * @return next iteration
     */
    private int leaveSecondaryLog(final SecondaryLog p_secondaryLog, final int p_counter) {
        if (p_counter > 0) {
            p_secondaryLog.resetReorgSegment();
            leaveSecLog(p_secondaryLog);
            m_allVersions.clear();
        }
        return 0;
    }

    /**
     * Grants the reorganization thread access to a secondary log.
     */
    public void grantAccessToCurrentLog() {
        if (m_reorgThreadWaits) {
            m_accessGrantedForReorgThread = true;
        }
    }

    /**
     * Sets the secondary log to reorganize next.
     *
     * @param p_secLog
     *         the Secondary Log
     * @param p_await
     *         whether to wait for completion of the reorganization or not
     */
    public void setLogToReorgImmediately(final SecondaryLog p_secLog, final boolean p_await) {

        if (p_await) {
            while (!m_reorganizationLock.tryLock()) {
                // Grant access for reorganization thread to avoid deadlock
                grantAccessToCurrentLog();
            }
            m_secLog = p_secLog;
            grantAccessToCurrentLog();
            while (p_secLog.equals(m_secLog)) {
                try {
                    if (!m_reorganizationFinishedCondition.await(10, TimeUnit.MICROSECONDS)) {
                        // Grant access for reorganization thread to avoid deadlock
                        grantAccessToCurrentLog();
                    }
                } catch (final InterruptedException ignore) {
                }
            }

            m_reorganizationLock.unlock();
        } else {
            m_requestLock.lock();
            m_reorganizationRequests.add(p_secLog);
            m_requestLock.unlock();
        }
    }

    /**
     * Get access to secondary log for reorganization thread.
     *
     * @param p_secLog
     *         the Secondary Log
     */
    private void getAccessToSecLog(final SecondaryLog p_secLog) {
        if (!p_secLog.isAccessed()) {
            p_secLog.setAccessFlag(true);

            m_reorgThreadWaits = true;
            while (!m_accessGrantedForReorgThread && !isInterrupted()) {
                Thread.yield();
            }
            m_accessGrantedForReorgThread = false;
            m_reorgThreadWaits = false;
        }
    }

    /**
     * Determines next log to process.
     *
     * @return secondary log
     */
    private SecondaryLog chooseLog() {
        SecondaryLog ret = null;
        final int numberOfLogs = m_backupRangeCatalog.getNumberOfLogs();
        long max = 0;
        long current;
        SecondaryLogBuffer[] secLogBuffers;
        SecondaryLogBuffer secLogBuffer;

        if (numberOfLogs > 0) {
            /*
             * Choose the largest log (or a log that has un-reorganized segments within an advanced eon)
             * To avoid starvation choose every third log randomly
             */
            if (m_counter++ < 2) {
                outerloop:
                for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
                    secLogBuffers = m_backupRangeCatalog.getAllSecondaryLogBuffers((short) i);
                    if (secLogBuffers != null) {
                        for (int j = 0; j < secLogBuffers.length; j++) {
                            secLogBuffer = secLogBuffers[j];
                            if (secLogBuffer != null) {
                                SecondaryLog secLog = secLogBuffer.getLog();
                                if (secLog.needToBeReorganized()) {
                                    ret = secLog;
                                    break outerloop;
                                }
                                current = secLog.getOccupiedSpace();
                                if (current > max) {
                                    max = current;
                                    ret = secLog;
                                }
                            }
                        }
                    }
                }
            } else {
                m_counter = 0;
            }
            if (m_counter == 0 && numberOfLogs > 1) {
                // Choose one secondary log randomly
                secLogBuffers = m_backupRangeCatalog.getSecondaryLogBuffersOfRandomNode();
                if (secLogBuffers != null && secLogBuffers.length > 0) {
                    int tries = 0;
                    while (ret == null && ++tries < 100) {
                        // Skip last log to speed up loading phase
                        SecondaryLogBuffer tmp = secLogBuffers[RandomUtils.getRandomValue(secLogBuffers.length - 2)];
                        if (tmp != null) {
                            ret = tmp.getLog();
                        }
                    }
                }
            }
        }

        return ret;
    }

    /**
     * Determines the next segment to reorganize.
     *
     * @return the chosen segment
     */
    private int chooseSegment(final SecondaryLog p_secondaryLog) {
        int ret = -1;
        int tries;
        double costBenefitRatio;
        double max = -1;
        SegmentHeader currentSegment;
        SegmentHeader[] segmentHeaders = p_secondaryLog.getSegmentHeaders();
        BitSet reorgVector = p_secondaryLog.getReorgVector();

        /*
         * Choose a segment based on the cost-benefit formula (the utilization does not contain an invalid counter).
         *
         * Every tenth segment is chosen randomly out of all segments that have not been reorganized in this eon.
         * Avoid segments that already have been reorganized within this epoch (-> m_reorgVector).
         */
        if (m_segmentReorgCounter++ == 10) {
            tries = (int) (m_secondaryLogSize / m_logSegmentSize * 2);
            while (true) {
                ret = RandomUtils.getRandomValue((int) (m_secondaryLogSize / m_logSegmentSize) - 1);
                if (segmentHeaders[ret] != null && segmentHeaders[ret].wasNotReorganized() && !reorgVector.get(ret) ||
                        --tries == 0) {
                    break;
                }
            }
            m_segmentReorgCounter = 0;
        }

        if (ret == -1 || segmentHeaders[ret] == null) {
            // Original cost-benefit ratio: ((1-u)*age)/(1+u)
            for (int i = 0; i < segmentHeaders.length; i++) {
                currentSegment = segmentHeaders[i];
                if (currentSegment != null && !reorgVector.get(i)) {
                    costBenefitRatio = currentSegment.getUtilization(m_logSegmentSize) *
                            currentSegment.getAge(p_secondaryLog.getCurrentTimeInSec());
                    if (costBenefitRatio > max) {
                        max = costBenefitRatio;
                        ret = i;
                    }
                }
            }
        }

        if (ret != -1) {
            // Mark segment as being reorganized in this epoch
            reorgVector.set(ret);
        }

        return ret;
    }

    /**
     * Reorganizes all segments.
     *
     * @param p_secondarayLog
     *         the secondary log to reorganize
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *         the lowest LID at the time the versions are read-in
     */
    private void reorganizeAll(final SecondaryLog p_secondarayLog, final DirectByteBufferWrapper p_bufferWrapper,
            final TemporaryVersionStorage p_allVersions, final long p_lowestLID) {
        SegmentHeader[] segmentHeaders = p_secondarayLog.getSegmentHeaders();
        for (int i = 0; i < segmentHeaders.length; i++) {
            if (segmentHeaders[i] != null && !Thread.currentThread().isInterrupted()) {
                if (!reorganizeSegment(p_secondarayLog, i, p_bufferWrapper, p_allVersions, p_lowestLID)) {
                    // Reorganization failed because of an I/O error -> abort
                    break;
                }
            }
        }

        p_secondarayLog.getReorgVector().clear();
    }

    /**
     * Reorganizes one segment by choosing the segment with best cost-benefit ratio.
     *
     * @param p_secondarayLog
     *         the secondary log to reorganize
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *         the lowest LID at the time the versions are read-in
     * @return whether the reorganization was successful or not
     */
    private boolean reorganizeIteratively(final SecondaryLog p_secondarayLog,
            final DirectByteBufferWrapper p_bufferWrapper, final TemporaryVersionStorage p_allVersions,
            final long p_lowestLID) {

        int segment = chooseSegment(p_secondarayLog);

        if (segment != -1) {
            return reorganizeSegment(p_secondarayLog, segment, p_bufferWrapper, p_allVersions, p_lowestLID);
        }

        return false;
    }

    /**
     * Reorganizes one given segment of a normal secondary log.
     *
     * @param p_segmentIndex
     *         the segments index
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         a hash table and int array with all versions for this secondary log
     * @param p_lowestCID
     *         the lowest CID at the time the versions were read-in
     * @return whether the reorganization was successful or not
     */
    private boolean reorganizeSegment(final SecondaryLog p_secondaryLog, final int p_segmentIndex,
            final DirectByteBufferWrapper p_bufferWrapper, final TemporaryVersionStorage p_allVersions,
            final long p_lowestCID) {
        boolean ret = true;
        int length;
        int readBytes = 0;
        int writtenBytes = 0;
        int segmentLength;
        long chunkID;
        long ageAllBytes = 0;
        ByteBuffer segmentData;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        short originalOwner = p_secondaryLog.getOriginalOwner();
        VersionBuffer versionBuffer =
                m_versionHandler.getVersionBuffer(p_secondaryLog.getOwner(), p_secondaryLog.getRangeID());

        if (p_segmentIndex != -1 && p_allVersions != null) {
            if (p_secondaryLog.assignReorgSegment(p_segmentIndex)) {

                try {

                    SOP_REORG_READ_SEGMENT.start();

                    segmentLength = p_secondaryLog.readSegment(p_bufferWrapper, p_segmentIndex);
                    segmentData = p_bufferWrapper.getBuffer();
                    ByteBuffer writeCopy = segmentData.duplicate();
                    writeCopy.order(ByteOrder.LITTLE_ENDIAN);

                    SOP_REORG_READ_SEGMENT.stop();

                    if (segmentLength > 0) {

                        SOP_REORG_PROCESS_SEGMENT.start();

                        while (readBytes < segmentLength && !Thread.currentThread().isInterrupted()) {
                            short type = (short) (segmentData.get(readBytes) & 0xFF);
                            logEntryHeader = AbstractSecLogEntryHeader.getHeader(type);
                            length = logEntryHeader.getHeaderSize(type) +
                                    logEntryHeader.getLength(type, segmentData, readBytes);
                            chunkID = logEntryHeader.getCID(type, segmentData, readBytes);
                            entryVersion = logEntryHeader.getVersion(type, segmentData, readBytes);

                            // Get current version
                            if (logEntryHeader.isMigrated()) {
                                currentVersion = p_allVersions.get(chunkID);
                            } else {
                                chunkID = ((long) originalOwner << 48) + chunkID;
                                currentVersion = p_allVersions.get(chunkID, p_lowestCID);
                            }
                            if (currentVersion == null || versionBuffer.getEpoch() == entryVersion.getEpoch()) {
                                // There is no entry in hash table or element is more current -> get latest
                                // version from cache (Epoch can only be 1 greater because there is no flushing during
                                // reorganization)
                                currentVersion = versionBuffer.getVersion(chunkID);
                            }

                            if (currentVersion == null || currentVersion.getVersion() == 0) {
                                LOGGER.error(
                                        "Version unknown for chunk 0x%X! Distance to lowest CID (0x%X): %d. Secondary" +
                                                " log: %s,%d; Current position in segment: %d", chunkID, p_lowestCID,
                                        chunkID - p_lowestCID, this, AbstractSecLogEntryHeader
                                                .getMaximumNumberOfVersions(m_secondaryLogSize / 2, 256, false),
                                        readBytes);

                            } else if (currentVersion.isEqual(
                                    entryVersion)) { /* TODO: do not delete log entry if epoch is current epoch */
                                // Compare current version with element
                                if (readBytes != writtenBytes) {
                                    segmentData.position(readBytes);
                                    int limit = segmentData.limit();
                                    segmentData.limit(readBytes + length);

                                    writeCopy.position(writtenBytes);
                                    writeCopy.put(segmentData);

                                    segmentData.limit(limit);
                                }
                                writtenBytes += length;

                                if (m_useTimestamps) {
                                    int entryAge = p_secondaryLog.getCurrentTimeInSec() -
                                            logEntryHeader.getTimestamp(type, segmentData, writtenBytes - length);
                                    if (entryAge < m_coldDataThreshold) {
                                        // Do not consider cold data for calculation
                                        ageAllBytes += entryAge * length;
                                    }
                                }

                                if (currentVersion.getEon() != versionBuffer.getEon()) {
                                    // Update eon in both versions
                                    logEntryHeader.flipEon(segmentData, writtenBytes - length);

                                    // Add to version buffer; all entries will get current eon during flushing
                                    versionBuffer.tryPut(chunkID, currentVersion.getVersion());
                                }
                            } else {
                                // Version, epoch and/or eon is different -> remove entry
                            }
                            readBytes += length;
                        }

                        SOP_REORG_PROCESS_SEGMENT.stop();

                        if (writtenBytes < readBytes && !Thread.currentThread().isInterrupted()) {

                            SOP_REORG_WRITE_SEGMENT.start();

                            if (writtenBytes > 0) {
                                p_secondaryLog.updateSegment(p_bufferWrapper, writtenBytes, p_segmentIndex);
                                if (m_useTimestamps) {
                                    // Calculate current age of segment
                                    p_secondaryLog.getSegmentHeader(p_segmentIndex)
                                            .setAge((int) (ageAllBytes / writtenBytes));
                                }
                            } else {
                                p_secondaryLog.freeSegment(p_segmentIndex);
                            }

                            SOP_REORG_WRITE_SEGMENT.stop();

                            SOP_REORG_BYTES_FREED.add(readBytes - writtenBytes);
                        }
                    }
                } catch (final IOException e) {

                    LOGGER.warn("Reorganization failed.", e);

                    ret = false;
                }
            }

            if (!Thread.currentThread().isInterrupted()) {
                if (readBytes - writtenBytes > 0) {
                    LOGGER.info(
                            "Freed %d bytes during reorganization of segment %d in range 0x%X,%d\t total log size: %d",
                            readBytes - writtenBytes, p_segmentIndex, p_secondaryLog.getOwner(),
                            p_secondaryLog.getRangeID(), p_secondaryLog.getOccupiedSpace() / 1024 / 1024);
                }
            } else {
                LOGGER.info("Interrupted during reorganization of segment %d in range 0x%X,%d\t total log size: %d",
                        p_segmentIndex, p_segmentIndex, p_secondaryLog.getOwner(), p_secondaryLog.getRangeID(),
                        p_secondaryLog.getOccupiedSpace() / 1024 / 1024);
            }
        }

        return ret;
    }

}
