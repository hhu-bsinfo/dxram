package de.hhu.bsinfo.utils.eval;

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
    }

    /**
     * Stop the watch and accumulate the resulting time
     * for avarage time calculation.
     */
    public void stopAndAccumulate() {
        m_endTime = System.nanoTime();
        m_accu += m_endTime - m_startTime;
        m_counter++;
    }

    /**
     * Get the average of accumulated times.
     *
     * @return Avarage of accumulated times in ns.
     */
    public long getAvarageOfAccumulated() {
        return m_accu / m_counter;
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
     *     Header to add to the time to print.
     * @param p_printReadable
     *     True to print a readable version (split into minutes, seconds etc).
     */
    public void print(final String p_header, final boolean p_printReadable) {
        long time;
        long nanoseconds;
        long microseconds;
        long milliseconds;
        long seconds;
        long minutes;
        long hours;

        if (m_endTime == -1) {
            m_endTime = System.nanoTime();
        }

        time = m_endTime - m_startTime;

        nanoseconds = time % 1000;
        time = time / 1000;

        microseconds = time % 1000;
        time = time / 1000;

        milliseconds = time % 1000;
        time = time / 1000;

        seconds = time % 60;
        time = time / 60;

        minutes = time % 60;
        time = time / 60;

        hours = time;

        if (p_printReadable) {
            System.out.println(
                "[" + p_header + "]: " + hours + "h " + minutes + "m " + seconds + "s " + milliseconds + "ms " + microseconds + "µs " + nanoseconds + "ns");
        } else {
            System.out.println("[" + p_header + "]: " + (m_endTime - m_startTime));
        }
    }
}
