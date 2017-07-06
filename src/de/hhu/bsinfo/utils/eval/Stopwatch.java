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

package de.hhu.bsinfo.utils.eval;

import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Methods to stop time
 *
 * @author Florian Klein, florian.klein@hhu.de, 20.06.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class Stopwatch {

    // Attributes
    private long m_startTime = 0;
    private long m_endTime = 0;
    private long m_accu = 0;
    private long m_counter = 0;
    private long m_best = Long.MAX_VALUE;
    private long m_worst = 0;

    // Constructors

    /**
     * Creates an instance of Stopwatch
     */
    public Stopwatch() {
    }

    // Methods

    /**
     * Starts the stop watch
     */
    public void start() {
        m_endTime = -1;
        m_startTime = System.nanoTime();
    }

    /**
     * Stops the stop watch
     */
    public void stop() {
        m_endTime = System.nanoTime();

        long tmp = m_endTime - m_startTime;

        if (tmp < m_best) {
            m_best = tmp;
        }

        if (tmp > m_worst) {
            m_worst = tmp;
        }
    }

    /**
     * Stop the watch and accumulate the resulting time
     * for average time calculation.
     *
     * @return Stopped time in ns
     */
    public long stopAndAccumulate() {
        m_endTime = System.nanoTime();

        long tmp = m_endTime - m_startTime;

        m_accu += tmp;
        m_counter++;

        if (tmp < m_best) {
            m_best = tmp;
        }

        if (tmp > m_worst) {
            m_worst = tmp;
        }

        return tmp;
    }

    /**
     * Get the average of accumulated times.
     *
     * @return Average of accumulated times in ns.
     */
    public long getAvarageOfAccumulated() {
        return m_accu / m_counter;
    }

    /**
     * Get the average of the accumulated time as a TimeUnit
     *
     * @return TimeUnit of the average of accumulated times
     */
    public TimeUnit getAverageOfAccumulatedTimeAsUnit() {
        return new TimeUnit(m_endTime - m_startTime, TimeUnit.NS);
    }

    /**
     * Get the stopped time.
     *
     * @return Stopped time in ns.
     */
    public long getTime() {
        return m_endTime - m_startTime;
    }

    /**
     * Get the recently stopped time
     *
     * @return TimeUnit of the recently stopped time
     */
    public TimeUnit getTimeAsUnit() {
        return new TimeUnit(m_endTime - m_startTime, TimeUnit.NS);
    }

    /**
     * Get the best time of all stopped times
     *
     * @return Best time in ns
     */
    public long getBestTime() {
        return m_best;
    }

    /**
     * Get the best time of all stopped times as a TimeUnit
     *
     * @return Best time as TimeUnit
     */
    public TimeUnit getBestTimeAsUnit() {
        return new TimeUnit(m_best, TimeUnit.NS);
    }

    /**
     * Get the worst time of all stopped times
     *
     * @return Worst time in ns
     */
    public long getWorstTime() {
        return m_worst;
    }

    /**
     * Get the worst time of all stopped times as a TimeUnit
     *
     * @return Worst time as TimeUnit
     */
    public TimeUnit getWorstTimeAsUnit() {
        return new TimeUnit(m_worst, TimeUnit.NS);
    }

    /**
     * Get the total accumulated time
     *
     * @return Accumulated time in ns
     */
    public long getAccumulatedTime() {
        return m_accu;
    }

    /**
     * Get the accumulated time
     *
     * @return TimeUnit of accumulated time
     */
    public TimeUnit getAccumulatedTimeAsUnit() {
        return new TimeUnit(m_accu, TimeUnit.NS);
    }

    /**
     * Get the stopped time as a string.
     *
     * @return Stopped time in ns as string.
     */
    public String getTimeStr() {
        return Long.toString(m_endTime - m_startTime);
    }

    /**
     * Prints the current time
     *
     * @param p_header
     *         Header to add to the time to print.
     */
    public void print(final String p_header) {
        System.out.printf("[%s] %s\n", p_header, toString());
    }

    @Override
    public String toString() {
        long time;

        if (m_endTime == -1) {
            m_endTime = System.nanoTime();
        }

        time = m_endTime - m_startTime;

        TimeUnit timeUnit = new TimeUnit(time, TimeUnit.NS);

        return timeUnit.toString();
    }
}
