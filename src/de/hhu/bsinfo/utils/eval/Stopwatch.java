
package de.hhu.bsinfo.utils.eval;

/**
 * Methods to stop time
 * @author Florian Klein
 *         20.06.2012
 */
public final class Stopwatch {

	// Attributes
	private long m_startTime;
	private long m_endTime;
	
	// Constructors
	/**
	 * Creates an instance of Stopwatch
	 */
	public Stopwatch() {}

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
	
	public long getTime() {
		return m_endTime - m_startTime;
	}
	
	public String getTimeStr() {
		return Long.toString(m_endTime - m_startTime);
	}

	/**
	 * Prints the current time
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
			System.out.println("[" + p_header + "]: " + hours + "h " + minutes + "m " + seconds + "s " + milliseconds + "ms " + microseconds + "µs " + nanoseconds + "ns");
		} else {
			System.out.println("[" + p_header + "]: " + (m_endTime - m_startTime));
		}
	}
}
