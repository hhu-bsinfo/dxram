/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.stats;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single operation tracks time, counters, averages etc
 * of one task/method call, for example: memory management
 * -> alloc operation
 * Each operation is part of a recorder.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class StatisticsOperation {
    public static final int INVALID_ID = -1;
    // stats per thread, avoids having locks
    private static int ms_blockSizeStatsMap = 100;
    private String m_name;
    private boolean m_enabled = true;
    private Stats[][] m_statsMap = new Stats[ms_blockSizeStatsMap][];
    private int m_statsMapBlockPos;
    private Lock m_mapLock = new ReentrantLock(false);

    /**
     * Constructor
     *
     * @param p_name
     *     Name of the operation.
     */
    StatisticsOperation(final String p_name) {
        m_name = p_name;

        m_statsMap[0] = new Stats[ms_blockSizeStatsMap];
        m_statsMapBlockPos = 1;
    }

    /**
     * Get the name of the operation
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Check if the operation is enabled
     *
     * @return True if enabled, false otherwise
     */
    public boolean isEnabled() {
        return m_enabled;
    }

    /**
     * Enable/disable recording of the operation
     *
     * @param p_val
     *     True to enable, false to disable
     */
    public void setEnabled(final boolean p_val) {
        m_enabled = p_val;
    }

    /**
     * Reset all recorded values
     */
    public void reset() {
        m_statsMap[0] = new Stats[ms_blockSizeStatsMap];
        m_statsMapBlockPos = 1;
    }

    /**
     * Call this when/before you start/enter the call/operation you want
     * to record.
     */
    public void enter() {
        if (!m_enabled) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
            m_mapLock.lock();
            while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
                m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
            }
            m_mapLock.unlock();
        }

        Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
        if (stats == null) {
            stats = new Stats(Thread.currentThread().getName());
            m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
        }

        stats.m_opCount++;
        stats.m_timeNsStart = System.nanoTime();
    }

    /**
     * Call this when/before you start/enter the call/operation you want
     * to record.
     *
     * @param p_val
     *     Value to added to the long counter.
     */
    public void enter(final long p_val) {
        if (!m_enabled) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
            m_mapLock.lock();
            while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
                m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
            }
            m_mapLock.unlock();
        }

        Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
        if (stats == null) {
            stats = new Stats(Thread.currentThread().getName());
            m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
        }

        stats.m_opCount++;
        stats.m_timeNsStart = System.nanoTime();
        stats.m_counter += p_val;
    }

    /**
     * Call this when/before you start/enter the call/operation you want
     * to record.
     *
     * @param p_val
     *     Value to added to the double counter.
     */
    public void enter(final double p_val) {
        if (!m_enabled) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
            m_mapLock.lock();
            while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
                m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
            }
            m_mapLock.unlock();
        }

        Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
        if (stats == null) {
            stats = new Stats(Thread.currentThread().getName());
            m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
        }

        stats.m_opCount++;
        stats.m_timeNsStart = System.nanoTime();
        stats.m_counter2 += p_val;
    }

    /**
     * Call this when/after you ended/left the call/operation.
     */
    public void leave() {
        if (!m_enabled) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
            m_mapLock.lock();
            while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
                m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
            }
            m_mapLock.unlock();
        }

        Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];

        long duration = System.nanoTime() - stats.m_timeNsStart;
        stats.m_totalTimeNs += duration;
        if (duration < stats.m_shortestTimeNs) {
            stats.m_shortestTimeNs = duration;
        }
        if (duration > stats.m_longestTimeNs) {
            stats.m_longestTimeNs = duration;
        }
    }

    @Override
    public String toString() {
        String str = '[' + m_name + " (enabled " + m_enabled + "): ";
        for (int i = 0; i < m_statsMapBlockPos; i++) {
            for (int j = 0; j < ms_blockSizeStatsMap; j++) {
                if (m_statsMap[i][j] != null) {
                    str += "\n\t\t" + m_statsMap[i][j];
                }
            }
        }

        return str;
    }

    /**
     * Internal state for an operation for statistics.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
     */
    private static final class Stats {
        private String m_threadName = "";

        private long m_opCount;
        private long m_totalTimeNs;
        private long m_shortestTimeNs = Long.MAX_VALUE;
        private long m_longestTimeNs = Long.MIN_VALUE;
        private long m_counter;
        private double m_counter2;

        // temporary stuff
        private long m_timeNsStart;

        /**
         * Constructor
         *
         * @param p_threadName
         *     Name of the thread
         */
        private Stats(final String p_threadName) {
            m_threadName = p_threadName;
        }

        /**
         * Get the operation count recorded (i.e. how often was enter called).
         *
         * @return Operation count.
         */
        public long getOpCount() {
            return m_opCount;
        }

        /**
         * Get the total amount of time spent inside the enter-leave section in ns.
         *
         * @return Total time in ns.
         */
        public long getTotalTimeNs() {
            return m_totalTimeNs;
        }

        /**
         * Get the total amount of time spent inside the enter-leave section in ms.
         *
         * @return Total time in ns.
         */
        public double getTotalTimeMs() {
            return m_totalTimeNs / 1000.0 / 1000.0;
        }

        /**
         * Get the shortest time spent inside the enter-leave section in ns.
         *
         * @return Shortest time in ns.
         */
        public long getShortestTimeNs() {
            return m_shortestTimeNs;
        }

        /**
         * Get the shortest time spent inside the enter-leave section in ms.
         *
         * @return Shortest time in ns.
         */
        public double getShortestTimeMs() {
            return m_shortestTimeNs / 1000.0 / 1000.0;
        }

        /**
         * Get the longest time spent inside the enter-leave section in ns.
         *
         * @return Longest time in ns.
         */
        public long getLongestTimeNs() {
            return m_longestTimeNs;
        }

        /**
         * Get the longest time spent inside the enter-leave section in ms.
         *
         * @return Longest time in ns.
         */
        public double getLongestTimeMs() {
            return m_longestTimeNs / 1000.0 / 1000.0;
        }

        /**
         * Get the average time spent inside the enter-leave section in ns.
         *
         * @return Average time in ns.
         */
        long getAverageTimeNs() {
            return m_totalTimeNs / m_opCount;
        }

        /**
         * Get the average time spent inside the enter-leave section in ms.
         *
         * @return Average time in ns.
         */
        double getAverageTimeMs() {
            return m_totalTimeNs / m_opCount / 1000.0 / 1000.0;
        }

        /**
         * Get the long counter. Depending on the operation, this is used for tracking different things.
         *
         * @return Long counter value.
         */
        public long getCounter() {
            return m_counter;
        }

        /**
         * Get the double counter. Depending on the operation, this is used for tracking different things.
         *
         * @return Double counter value.
         */
        public double getCounter2() {
            return m_counter2;
        }

        /**
         * Calculate the number of operations per second.
         *
         * @return Number of operations per second.
         */
        float getOpsPerSecond() {

            return (float) (1000.0 * 1000.0 * 1000.0 / ((double) m_totalTimeNs / m_opCount));
        }

        @Override
        public String toString() {
            return "Stats[" + m_threadName + "](m_opCount, " + m_opCount + ")(avgTimeMs, " + getAverageTimeMs() + ")(m_totalTimeMs, " + getTotalTimeMs() +
                ")(m_shortestTimeMs, " + getShortestTimeMs() + ")(m_longestTimeMs, " + getLongestTimeMs() + ")(avgTimeMs, " + getAverageTimeMs() +
                ")(opsPerSecond, " + getOpsPerSecond() + ")(m_counter, " + m_counter + ")(m_counter2, " + m_counter2 + ')';
        }
    }
}
