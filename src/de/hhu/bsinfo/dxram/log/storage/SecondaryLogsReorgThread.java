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

package de.hhu.bsinfo.dxram.log.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxutils.RandomUtils;

/**
 * Reorganization thread
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.06.2014
 */
public final class SecondaryLogsReorgThread extends Thread {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLogsReorgThread.class.getSimpleName());

    // Attributes
    private final LinkedHashSet<SecondaryLog> m_reorganizationRequests;
    private LogComponent m_logComponent;
    private long m_secondaryLogSize;
    private int m_activateReorganizationThreshold;
    private TemporaryVersionsStorage m_allVersions;
    private ReentrantLock m_reorganizationLock;
    private Condition m_reorganizationFinishedCondition;
    private ReentrantLock m_requestLock;
    private ReentrantLock m_recoveryLock;
    private DirectByteBufferWrapper m_reorgSegmentData;

    private byte m_counter;
    private final int m_iterationsPerLog;
    private volatile SecondaryLog m_secLog;
    private volatile boolean m_reorgThreadWaits;
    private volatile boolean m_accessGrantedForReorgThread;

    // Constructors
    private volatile boolean m_shutdown;

    // Setter

    /**
     * Creates an instance of SecondaryLogsReorgThread
     *
     * @param p_logComponent
     *         the log component
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_utilizationActivateReorganization
     *         the threshold to consider a log for reorganization
     */
    public SecondaryLogsReorgThread(final LogComponent p_logComponent, final long p_secondaryLogSize,
            final int p_logSegmentSize, final int p_utilizationActivateReorganization) {
        m_logComponent = p_logComponent;
        m_secondaryLogSize = p_secondaryLogSize;
        m_iterationsPerLog = (int) (p_secondaryLogSize / p_logSegmentSize * 0.33f);
        m_activateReorganizationThreshold =
                (int) ((float) p_utilizationActivateReorganization / 100 * m_secondaryLogSize);

        m_allVersions = new TemporaryVersionsStorage(m_secondaryLogSize);

        m_reorganizationLock = new ReentrantLock(false);
        m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();

        m_reorganizationRequests = new LinkedHashSet<SecondaryLog>();
        m_requestLock = new ReentrantLock(false);

        m_recoveryLock = new ReentrantLock(false);

        m_reorgSegmentData = new DirectByteBufferWrapper(p_logSegmentSize, true);

        m_counter = 0;
    }

    /**
     * Get access to secondary log for reorganization thread
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
     * Shutdown
     */
    public void shutdown() {
        m_shutdown = true;
    }

    /**
     * Block the reorganization thread
     * Is called during recovery.
     */
    public void block() {
        while (!m_recoveryLock.tryLock()) {
            interrupt();
        }
    }

    /**
     * Unblock the reorganization thread
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
                        lowestLID = secondaryLog.getCurrentVersions(m_allVersions, true);
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
                // #if LOGGER == TRACE
                LOGGER.trace("Going to reorganize %s", secondaryLog.getRangeID());
                // #endif /* LOGGER == TRACE */
                getAccessToSecLog(secondaryLog);

                if (!interrupted()) {
                    final long start = System.currentTimeMillis();
                    if (!secondaryLog.reorganizeIteratively(m_reorgSegmentData, m_allVersions, lowestLID)) {
                        // Reorganization failed because of an I/O error -> switch log
                        counter = m_iterationsPerLog;
                    }

                    if (!interrupted()) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Time to reorganize segment: %d", System.currentTimeMillis() - start);
                        // #endif /* LOGGER == TRACE */
                    } else {
                        // #if LOGGER == TRACE
                        LOGGER.debug("Reorganization of segment was interrupted! Time: %d",
                                System.currentTimeMillis() - start);
                        // #endif /* LOGGER == TRACE */
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
     * Process urgent request by reorganizing the entire secondary log
     *
     * @lock m_reorganizationLock must be acquired
     */
    private void processUrgentRequest() {
        // Reorganize complete secondary log
        SecondaryLog secondaryLog = m_secLog;

        // #if LOGGER >= DEBUG
        LOGGER.debug("Got urgent reorganization request for %s", m_secLog.getRangeID());
        // #endif /* LOGGER >= DEBUG */

        m_reorganizationLock.unlock();
        getAccessToSecLog(secondaryLog);

        if (!interrupted()) {
            m_reorganizationLock.lock();

            long lowestLID = secondaryLog.getCurrentVersions(m_allVersions, true);
            if (!interrupted()) {

                // TODO: flush primary and secondary log buffers

                secondaryLog.reorganizeAll(m_reorgSegmentData, m_allVersions, lowestLID);
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
     * Process low priority requests by reorganizing the entire secondary log
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
            // #if LOGGER == DEBUG
            LOGGER.debug("Got reorganization request for %s. Queue length: %d", secondaryLog.getRangeID(),
                    m_reorganizationRequests.size());
            // #endif /* LOGGER == DEBUG */
            m_requestLock.unlock();

            long start = System.currentTimeMillis();
            // Reorganize complete secondary log
            getAccessToSecLog(secondaryLog);
            if (!interrupted()) {
                lowestLID = secondaryLog.getCurrentVersions(m_allVersions, true);
                if (!interrupted()) {

                    // TODO: flush primary and secondary log buffers

                    int counter = 0;
                    while (secondaryLog.getOccupiedSpace() > m_activateReorganizationThreshold ||
                            secondaryLog.needToBeReorganized()) {
                        // Reorganize if any updates arrived, only
                        secondaryLog.reorganizeIteratively(m_reorgSegmentData, m_allVersions, lowestLID);
                        if (++counter == m_iterationsPerLog) {
                            break;
                        }
                    }
                }
            }
            secondaryLog.resetReorgSegment();
            leaveSecLog(secondaryLog);
            m_allVersions.clear();

            // #if LOGGER == TRACE
            LOGGER.trace("Time to reorganize complete log: %d", System.currentTimeMillis() - start);
            // #endif /* LOGGER == TRACE */
        }
    }

    /**
     * Reset data structures and leave secondary log
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
     * Grants the reorganization thread access to a secondary log
     */
    void grantAccessToCurrentLog() {
        if (m_reorgThreadWaits) {
            m_accessGrantedForReorgThread = true;
        }
    }

    /**
     * Sets the secondary log to reorganize next
     *
     * @param p_secLog
     *         the Secondary Log
     * @param p_await
     *         whether to wait for completion of the reorganization or not
     * @throws InterruptedException
     *         if caller is interrupted
     */
    void setLogToReorgImmediately(final SecondaryLog p_secLog, final boolean p_await) throws InterruptedException {

        if (p_await) {
            while (!m_reorganizationLock.tryLock()) {
                // Grant access for reorganization thread to avoid deadlock
                grantAccessToCurrentLog();
            }
            m_secLog = p_secLog;
            grantAccessToCurrentLog();
            while (p_secLog.equals(m_secLog)) {
                if (!m_reorganizationFinishedCondition.await(10, TimeUnit.MICROSECONDS)) {
                    // Grant access for reorganization thread to avoid deadlock
                    grantAccessToCurrentLog();
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
     * Get access to secondary log for reorganization thread
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

    // Methods

    /**
     * Determines next log to process
     *
     * @return secondary log
     */
    private SecondaryLog chooseLog() {
        SecondaryLog ret = null;
        int numberOfLogs = 0;
        long max = 0;
        long current;
        LogCatalog cat;
        LogCatalog[] allCats;
        ArrayList<LogCatalog> cats;
        SecondaryLog[] secLogs;
        SecondaryLog secLog;

        allCats = m_logComponent.getAllLogCatalogs();
        if (allCats != null) {
            cats = new ArrayList<>();
            for (int i = 0; i < allCats.length; i++) {
                cat = allCats[i];
                if (cat != null) {
                    cats.add(cat);
                }
            }

            for (LogCatalog ct : cats) {
                numberOfLogs += ct.getNumberOfLogs();
            }

            /*
             * Choose the largest log (or a log that has un-reorganized segments within an advanced eon)
             * To avoid starvation choose every third log randomly
             */
            if (m_counter++ < 2) {
                outerloop:
                for (LogCatalog currentCat : cats) {
                    secLogs = currentCat.getAllLogs();
                    for (int j = 0; j < secLogs.length; j++) {
                        secLog = secLogs[j];
                        if (secLog != null) {
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
            } else {
                m_counter = 0;
            }
            if (m_counter == 0 && !cats.isEmpty() && numberOfLogs > 1) {
                // Choose one secondary log randomly
                cat = cats.get(RandomUtils.getRandomValue(cats.size() - 1));
                secLogs = cat.getAllLogs();
                if (secLogs.length > 0) {
                    int tries = 0;
                    while (ret == null && ++tries < 100) {
                        // Skip last log to speed up loading phase
                        ret = secLogs[RandomUtils.getRandomValue(secLogs.length - 2)];
                    }
                }
            }
        }

        return ret;
    }

}
