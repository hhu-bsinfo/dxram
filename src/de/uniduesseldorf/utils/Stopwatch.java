
package de.uniduesseldorf.utils;

/**
 * Methods to stop time
 * @author Florian Klein
 *         20.06.2012
 */
public final class Stopwatch {

	// Attributes
	private long m_time;

	// Constructors
	/**
	 * Creates an instance of Stopwatch
	 */
	public Stopwatch() {}

	// Methods
	/**
	 * Starts the stop watch
	 */
	public synchronized void start() {
		m_time = System.nanoTime();
	}

	/**
	 * Stops the stop watch
	 */
	public synchronized void stop() {
		m_time = 0;
	}

	/**
	 * Prints the current time
	 */
	public synchronized void print() {
		long time;
		long nanoseconds;
		long microseconds;
		long milliseconds;
		long seconds;
		long minutes;
		long hours;

		time = System.nanoTime() - m_time;

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

		System.out.println("*\nTime:\t" + hours + "h " + minutes + "m " + seconds + "s " + milliseconds + "ms " + microseconds + "µs " + nanoseconds + "ns\n*");
	}

	/**
	 * Prints the current time and stops the stop watch
	 */
	public synchronized void printAndStop() {
		print();
		stop();
	}

}
