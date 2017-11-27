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

package de.hhu.bsinfo.utils.stats;

import java.util.ArrayList;
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
    private static int ms_blockSizeStatsMap = 1000;
    private String m_name;
    private boolean m_enabled = true;
    private Stats[][] m_statsMap = new Stats[10000][0];
    private int m_statsMapBlockPos;
    private Lock m_mapLock = new ReentrantLock(false);

    /**
     * Constructor
     *
     * @param p_name
     *         Name of the operation.
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
     *         True to enable, false to disable
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
     *         Value to added to the long counter.
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
     *         Value to added to the double counter.
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
     * Record previously determined time span. Stores all values to calculate percentiles.
     *
     * @param p_val
     *         Value to record.
     */
    public void record(final long p_val) {
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

        StatsPercentile stats = (StatsPercentile) m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
        if (stats == null) {
            stats = new StatsPercentile(Thread.currentThread().getName());
            m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
        }

        stats.m_totalTimeNs += p_val;
        if (p_val < stats.m_shortestTimeNs) {
            stats.m_shortestTimeNs = p_val;
        }
        if (p_val > stats.m_longestTimeNs) {
            stats.m_longestTimeNs = p_val;
        }
        stats.m_opCount++;

        stats.register(p_val);
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
    private static class Stats {
        private String m_threadName = "";

        long m_opCount;
        long m_totalTimeNs;
        long m_shortestTimeNs = Long.MAX_VALUE;
        long m_longestTimeNs = Long.MIN_VALUE;
        private long m_counter;
        private double m_counter2;

        // temporary stuff
        private long m_timeNsStart;

        /**
         * Constructor
         *
         * @param p_threadName
         *         Name of the thread
         */
        Stats(final String p_threadName) {
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
                    ")(m_shortestTimeMs, " + getShortestTimeMs() + ")(m_longestTimeMs, " + getLongestTimeMs() + ")(opsPerSecond, " + getOpsPerSecond() +
                    ")(m_counter, " + m_counter + ")(m_counter2, " + m_counter2 + ')';
        }
    }

    /**
     * Internal state for an operation storing percentiles for statistics.
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 27.11.2017
     */
    private static final class StatsPercentile extends Stats {

        private static final int SLOT_SIZE = 100000;

        private ArrayList<long[]> m_slots;
        private int m_index;

        /**
         * Constructor
         *
         * @param p_threadName
         *         Name of the thread
         */
        private StatsPercentile(final String p_threadName) {
            super(p_threadName);

            m_slots = new ArrayList<>();
            long[] arr = new long[SLOT_SIZE];
            m_slots.add(arr);

            m_index = 0;
        }

        /**
         * Register value.
         *
         * @param p_value
         *         the value
         */
        void register(final long p_value) {
            long[] arr = m_slots.get(m_slots.size() - 1);
            if (m_index == SLOT_SIZE) {
                arr = new long[SLOT_SIZE];
                m_slots.add(arr);
                m_index = 0;
            }

            arr[m_index++] = p_value;
        }

        /**
         * Calculate percentile.
         *
         * @param p_percentage
         *         the percentage
         * @return the percentile
         */
        double getPercentile(final float p_percentage) {
            if (p_percentage <= 0.0 || p_percentage >= 1.0) {
                throw new IllegalArgumentException("Percentage must be in (0.0, 1.0)!");
            }

            int size = (m_slots.size() - 1) * SLOT_SIZE + m_index;
            int index = (int) Math.ceil(p_percentage * size) - 1;
            return m_slots.get(index / SLOT_SIZE)[index % SLOT_SIZE] / 1000.0 / 1000.0;
        }

        /**
         * Sort all registered values (ascending).
         */
        private void sortValues() {
            quickSort(0, (m_slots.size() - 1) * SLOT_SIZE + m_index - 1);
        }

        /**
         * Quicksort implementation.
         *
         * @param p_lowerIndex
         *         the lower index
         * @param p_higherIndex
         *         the higher index
         */
        private void quickSort(int p_lowerIndex, int p_higherIndex) {
            int i = p_lowerIndex;
            int j = p_higherIndex;
            int index = p_lowerIndex + (p_higherIndex - p_lowerIndex) / 2;
            long pivot = m_slots.get(index / SLOT_SIZE)[index % SLOT_SIZE];
            while (i <= j) {
                while (m_slots.get(i / SLOT_SIZE)[i % SLOT_SIZE] < pivot) {
                    i++;
                }
                while (m_slots.get(j / SLOT_SIZE)[j % SLOT_SIZE] > pivot) {
                    j--;
                }
                if (i <= j) {
                    exchangeNumbers(i, j);
                    i++;
                    j--;
                }
            }
            if (p_lowerIndex < j) {
                quickSort(p_lowerIndex, j);
            }
            if (i < p_higherIndex) {
                quickSort(i, p_higherIndex);
            }
        }

        /**
         * Helper method for quicksort. Exchange two values.
         *
         * @param p_i
         *         first index
         * @param p_j
         *         second index
         */
        private void exchangeNumbers(int p_i, int p_j) {
            long temp = m_slots.get(p_i / SLOT_SIZE)[p_i % SLOT_SIZE];
            m_slots.get(p_i / SLOT_SIZE)[p_i % SLOT_SIZE] = m_slots.get(p_j / SLOT_SIZE)[p_j % SLOT_SIZE];
            m_slots.get(p_j / SLOT_SIZE)[p_j % SLOT_SIZE] = temp;
        }

        @Override
        public String toString() {
            sortValues();
            return super.toString() + "(95th percentile, " + getPercentile(0.95f) + ")(99th percentile, " + getPercentile(0.99f) + ")(99.9th percentile, " +
                    getPercentile(0.999f) + ')';
        }
    }
}
