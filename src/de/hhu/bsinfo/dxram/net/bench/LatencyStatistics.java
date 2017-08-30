package de.hhu.bsinfo.dxram.net.bench;

import de.hhu.bsinfo.utils.eval.Stopwatch;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Created by nothaas on 7/3/17.
 */
public class LatencyStatistics {
    private static final int MAX_THREADS = 1000;

    private static final String[] MS_LATENCY_UNITS =
            new String[] {"< 1 us", "< 2 us", "< 5 us", "< 10 us", "< 15 us", "< 20 us", "< 25 us", "< 50 us", "< 100 us", "< 250 us", "< 500 us", "< 1 ms",
                    "< 2 ms", "< 5 ms", "< 10 ms", "< 25 ms", "< 50 ms", "< 100 ms", "< 250 ms", "< 500 ms", "< 1 sec", ">= 1 sec"};

    private final Stopwatch[] m_stopwatch;
    private final long[][] m_latencyCounts;

    public LatencyStatistics() {
        this(MAX_THREADS);
    }

    public LatencyStatistics(final int p_threadCount) {
        m_stopwatch = new Stopwatch[p_threadCount];
        for (int i = 0; i < m_stopwatch.length; i++) {
            m_stopwatch[i] = new Stopwatch();
        }

        m_latencyCounts = new long[p_threadCount][MS_LATENCY_UNITS.length];

    }

    public void enter(final int p_threadIndex) {
        m_stopwatch[p_threadIndex].start();
    }

    public void exit(final int p_threadIndex) {
        long tmp = m_stopwatch[p_threadIndex].stopAndAccumulate();
        addToLatencyCounters(p_threadIndex, tmp);
    }

    public TimeUnit getTotalTime(final int p_threadIndex) {
        return m_stopwatch[p_threadIndex].getAccumulatedTimeAsUnit();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < m_stopwatch.length; i++) {
            if (m_stopwatch[i].getTime() > 0) {
                builder.append("[Thread ");
                builder.append(i);
                builder.append("]: avg. ");
                builder.append(m_stopwatch[i].getAverageOfAccumulatedTimeAsUnit());
                builder.append(", best ");
                builder.append(m_stopwatch[i].getBestTimeAsUnit());
                builder.append(", worst ");
                builder.append(m_stopwatch[i].getWorstTimeAsUnit());

                for (int j = 0; j < m_latencyCounts[i].length; j++) {
                    if (m_latencyCounts[i][j] > 0) {
                        builder.append('\n');
                        builder.append(MS_LATENCY_UNITS[j]);
                        builder.append(": ");
                        builder.append(m_latencyCounts[i][j]);
                    }
                }
                builder.append('\n');
            }
        }

        return builder.toString();
    }

    private void addToLatencyCounters(final int p_threadId, final long p_timeNs) {
        // less than 1 us
        if (p_timeNs / 1000 == 0) {
            m_latencyCounts[p_threadId][0]++;
        }
        // less than 1 ms
        else if (p_timeNs / 1000 / 1000 == 0) {
            long tmp = p_timeNs / 1000;

            if (tmp < 2) {
                // less than 2 us
                m_latencyCounts[p_threadId][1]++;
            } else if (tmp < 5) {
                // less than 5 us
                m_latencyCounts[p_threadId][2]++;
            } else if (tmp < 10) {
                // less than 10 us
                m_latencyCounts[p_threadId][3]++;
            } else if (tmp < 15) {
                // less than 10 us
                m_latencyCounts[p_threadId][4]++;
            } else if (tmp < 20) {
                // less than 10 us
                m_latencyCounts[p_threadId][5]++;
            } else if (tmp < 25) {
                // less than 25 us
                m_latencyCounts[p_threadId][6]++;
            } else if (tmp < 50) {
                // less than 50 us
                m_latencyCounts[p_threadId][7]++;
            } else if (tmp < 100) {
                // less than 100 us
                m_latencyCounts[p_threadId][8]++;
            } else if (tmp < 250) {
                // less than 250 us
                m_latencyCounts[p_threadId][9]++;
            } else if (tmp < 500) {
                // less than 500 us
                m_latencyCounts[p_threadId][10]++;
            } else {
                // less than 1000 us/1 ms
                m_latencyCounts[p_threadId][11]++;
            }
        }
        // less than 1 sec
        else if (p_timeNs / 1000 / 1000 / 1000 == 0) {
            long tmp = p_timeNs / 1000 / 1000;

            if (tmp < 2) {
                // less than 2 ms
                m_latencyCounts[p_threadId][12]++;
            } else if (tmp < 5) {
                // less than 5 ms
                m_latencyCounts[p_threadId][13]++;
            } else if (tmp < 10) {
                // less than 10 ms
                m_latencyCounts[p_threadId][14]++;
            } else if (tmp < 25) {
                // less than 25 ms
                m_latencyCounts[p_threadId][15]++;
            } else if (tmp < 50) {
                // less than 50 ms
                m_latencyCounts[p_threadId][16]++;
            } else if (tmp < 100) {
                // less than 100 ms
                m_latencyCounts[p_threadId][17]++;
            } else if (tmp < 250) {
                // less than 250 ms
                m_latencyCounts[p_threadId][18]++;
            } else if (tmp < 500) {
                // less than 500 ms
                m_latencyCounts[p_threadId][19]++;
            } else {
                // less than 1000 ms/1 sec
                m_latencyCounts[p_threadId][20]++;
            }
        }
        // 1 sec+
        else {
            m_latencyCounts[p_threadId][21]++;
        }
    }
}
