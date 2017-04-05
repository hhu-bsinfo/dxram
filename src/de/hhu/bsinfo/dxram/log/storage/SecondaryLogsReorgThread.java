/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
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
import de.hhu.bsinfo.utils.RandomUtils;

/**
 * Reorganization thread
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.06.2014
 */
public final class SecondaryLogsReorgThread extends Thread {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLogsReorgThread.class.getSimpleName());
    private final LinkedHashSet<SecondaryLog> m_reorganizationRequests;
    // Attributes
    private LogComponent m_logComponent;
    private long m_secondaryLogSize;
    private TemporaryVersionsStorage m_allVersions;
    private ReentrantLock m_reorganizationLock;
    private Condition m_reorganizationFinishedCondition;
    private ReentrantLock m_requestLock;
    private byte[] m_reorgSegmentData;
    private byte m_counter;
    private boolean m_isRandomChoice;
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
     *     the log component
     * @param p_secondaryLogSize
     *     the secondary log size
     * @param p_logSegmentSize
     *     the segment size
     */
    public SecondaryLogsReorgThread(final LogComponent p_logComponent, final long p_secondaryLogSize, final int p_logSegmentSize) {
        m_logComponent = p_logComponent;
        m_secondaryLogSize = p_secondaryLogSize;

        m_allVersions = new TemporaryVersionsStorage(m_secondaryLogSize);

        m_reorganizationLock = new ReentrantLock(false);
        m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();

        m_reorganizationRequests = new LinkedHashSet<SecondaryLog>();
        m_requestLock = new ReentrantLock(false);

        m_reorgSegmentData = new byte[p_logSegmentSize];

        m_counter = 0;
    }

    /**
     * Get access to secondary log for reorganization thread
     *
     * @param p_secLog
     *     the Secondary Log
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
        m_reorganizationLock.lock();
    }

    @Override
    public void run() {
        final int iterationsPerLog = 20;
        int counter = 0;
        long lowestLID = 0;
        Iterator<SecondaryLog> iter;
        SecondaryLog secondaryLog = null;

        while (!m_shutdown) {
            m_reorganizationLock.lock();
            // Check if there is an urgent reorganization request -> reorganize complete secondary log and signal
            if (m_secLog != null) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Got urgent reorganization request for %s", m_secLog.getRangeID());
                // #endif /* LOGGER >= DEBUG */

                // Leave current secondary log
                if (counter > 0) {
                    secondaryLog.resetReorgSegment();
                    leaveSecLog(secondaryLog);
                    m_allVersions.clear();
                    counter = 0;
                }

                // Reorganize complete secondary log
                secondaryLog = m_secLog;

                m_reorganizationLock.unlock();
                getAccessToSecLog(secondaryLog);
                m_reorganizationLock.lock();

                lowestLID = secondaryLog.getCurrentVersions(m_allVersions);
                secondaryLog.reorganizeAll(m_reorgSegmentData, m_allVersions, lowestLID);
                secondaryLog.resetReorgSegment();
                leaveSecLog(secondaryLog);
                m_allVersions.clear();

                m_secLog = null;
                m_reorganizationFinishedCondition.signalAll();
                m_reorganizationLock.unlock();
                continue;
            }
            m_reorganizationLock.unlock();

            // Check if there are normal reorganization requests -> reorganize complete secondary logs
            m_requestLock.lock();
            if (!m_reorganizationRequests.isEmpty()) {
                m_requestLock.unlock();
                // Leave current secondary log
                if (counter > 0) {
                    secondaryLog.resetReorgSegment();
                    leaveSecLog(secondaryLog);
                    m_allVersions.clear();
                    counter = 0;
                }

                // Process all reorganization requests
                while (!m_reorganizationRequests.isEmpty() && !m_shutdown) {
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
                    // #if LOGGER == TRACE
                    LOGGER.trace("Got reorganization request for %s. Queue length: %d", secondaryLog.getRangeID(), m_reorganizationRequests.size());
                    // #endif /* LOGGER == TRACE */
                    m_requestLock.unlock();

                    // Reorganize complete secondary log
                    getAccessToSecLog(secondaryLog);
                    lowestLID = secondaryLog.getCurrentVersions(m_allVersions);
                    secondaryLog.reorganizeAll(m_reorgSegmentData, m_allVersions, lowestLID);
                    secondaryLog.resetReorgSegment();
                    leaveSecLog(secondaryLog);
                    m_allVersions.clear();
                }
                continue;
            }
            m_requestLock.unlock();

            if (counter == 0) {
                // This is the first iteration -> choose secondary log and gather versions
                secondaryLog = chooseLog();
                if (secondaryLog != null && (secondaryLog.getOccupiedSpace() > m_secondaryLogSize / 2 || m_isRandomChoice)) {
                    getAccessToSecLog(secondaryLog);
                    lowestLID = secondaryLog.getCurrentVersions(m_allVersions);
                } else {
                    // Nothing to do -> wait for a while to reduce cpu load
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignored) {
                    }
                    continue;
                }
            }

            // Reorganize one segment
            // #if LOGGER == TRACE
            LOGGER.trace("Going to reorganize %s", secondaryLog.getRangeID());
            // #endif /* LOGGER == TRACE */
            getAccessToSecLog(secondaryLog);

            final long start = System.currentTimeMillis();
            if (!secondaryLog.reorganizeIteratively(m_reorgSegmentData, m_allVersions, lowestLID)) {
                // Reorganization failed because of an I/O error -> switch log
                counter = iterationsPerLog;
            }
            // #if LOGGER == TRACE
            LOGGER.trace("Time to reorganize segment: %d", System.currentTimeMillis() - start);
            // #endif /* LOGGER == TRACE */

            if (counter++ == iterationsPerLog) {
                // This was the last iteration for current secondary log -> clean-up
                secondaryLog.resetReorgSegment();
                leaveSecLog(secondaryLog);
                m_allVersions.clear();
                counter = 0;
            }
        }
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
     * Unblock the reorganization thread
     * Is called during recovery.
     */
    void unblock() {
        m_reorganizationLock.unlock();
    }

    /**
     * Sets the secondary log to reorganize next
     *
     * @param p_secLog
     *     the Secondary Log
     * @param p_await
     *     whether to wait for completion of the reorganization or not
     * @throws InterruptedException
     *     if caller is interrupted
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
     *     the Secondary Log
     */
    private void getAccessToSecLog(final SecondaryLog p_secLog) {
        if (!p_secLog.isAccessed()) {
            p_secLog.setAccessFlag(true);

            m_reorgThreadWaits = true;
            while (!m_accessGrantedForReorgThread) {
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
            m_isRandomChoice = false;
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
            m_isRandomChoice = true;
            // Choose one secondary log randomly
            cat = cats.get(RandomUtils.getRandomValue(cats.size() - 1));
            secLogs = cat.getAllLogs();
            if (secLogs.length > 0) {
                while (ret == null) {
                    // Skip last log to speed up loading phase
                    ret = secLogs[RandomUtils.getRandomValue(secLogs.length - 2)];
                }
            }
        }

        return ret;
    }

}
