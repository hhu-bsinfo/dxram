
package de.uniduesseldorf.dxram.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;

/**
 * Manages multiple statistics
 * @author klein 26.03.2015
 */
public final class StatisticsManager {

	// Attributes
	private static Map<String, Statistic> m_statistics;
	private static boolean m_printDetails = true;
	private static Lock m_lock = new ReentrantLock(false);

	private static Timer m_timer;

	// Constructor
	/**
	 * Creates an instance of StatisticsManager
	 */
	private StatisticsManager() {}

	// Getters
	/**
	 * Checks if details should be printed
	 * @return true if details should be printed
	 */
	public static boolean isPrintDetails() {
		return m_printDetails;
	}

	// Setters
	/**
	 * Set the print details option
	 * @param p_printDetails
	 *            if true details will be printed
	 */
	public static void setPrintDetails(final boolean p_printDetails) {
		m_printDetails = p_printDetails;
	}

	// Methods
	/**
	 * Registers a statistic
	 * @param p_name
	 *            the statistic name
	 * @param p_statistic
	 *            the statistic
	 */
	public static void registerStatistic(final String p_name, final Statistic p_statistic) {
		Contract.checkNotNull(p_name, "no name given");
		Contract.checkNotNull(p_statistic, "no statistic given");

		m_lock.lock();

		if (m_statistics == null) {
			m_statistics = new HashMap<>();
		}
		m_statistics.put(p_name, p_statistic);

		m_lock.unlock();
	}

	/**
	 * Setups the output
	 * @param p_period
	 *            the period between two outputs
	 */
	public static void setupOutput(final long p_period) {
		setupOutput(p_period, System.out);
	}

	/**
	 * Setups the output
	 * @param p_period
	 *            the period between two outputs
	 * @param p_file
	 *            the output file
	 * @throws FileNotFoundException
	 *             if the file could not be found
	 */
	public static void setupOutput(final long p_period, final File p_file) throws FileNotFoundException {
		setupOutput(p_period, new PrintStream(p_file));
	}

	/**
	 * Setups the output
	 * @param p_period
	 *            the period between two outputs
	 * @param p_stream
	 *            the output stream
	 */
	public static void setupOutput(final long p_period, final PrintStream p_stream) {
		Contract.check(p_period > 0, "invalid period");
		Contract.checkNotNull(p_stream, "no stream given");

		if (!NodeID.getRole().equals(Role.MONITOR)) {

			if (m_timer != null) {
				m_timer.cancel();
			}

			m_timer = new Timer(StatisticsManager.class.getSimpleName(), true);
			m_timer.scheduleAtFixedRate(new StatisticsTask(p_stream), 0, p_period * 1000);
		}
	}

	/**
	 * Closes the manager
	 */
	public static void close() {
		m_timer.cancel();
	}

	// Classes
	/**
	 * Represents a statistic
	 * @author klein 26.03.2015
	 */
	public interface Statistic {

		// Methods
		/**
		 * Gets the statistic values
		 * @param p_withDetails
		 *            if true, details will be included
		 * @return the statistic values
		 */
		List<StatisticEntry> getValues(final boolean p_withDetails);

	}

	/**
	 * Represents a statistic value
	 * @author klein 26.03.2015
	 */
	public static final class StatisticEntry implements Comparable<StatisticEntry> {

		// Attributes
		private int m_position;
		private String m_name;
		private String m_value;

		// Constructors
		/**
		 * Creates an instance of StatisticEntry
		 * @param p_position
		 *            the position
		 * @param p_name
		 *            the name
		 * @param p_value
		 *            the value
		 */
		public StatisticEntry(final int p_position, final String p_name, final String p_value) {
			m_position = p_position;
			m_name = p_name;
			m_value = p_value;
		}

		// Getters
		/**
		 * Gets the position
		 * @return the position
		 */
		private int getPosition() {
			return m_position;
		}

		/**
		 * Gets the name
		 * @return the name
		 */
		private String getName() {
			return m_name;
		}

		/**
		 * Gets the value
		 * @return the value
		 */
		private String getValue() {
			return m_value;
		}

		// Methods
		@Override
		public int compareTo(final StatisticEntry p_other) {
			Contract.checkNotNull(p_other, "no object given");

			return m_position - p_other.getPosition();
		}

	}

	/**
	 * Represents a statistic for an operation
	 * @author klein 26.03.2015
	 */
	public static final class OperationStatistic {

		// Constants
		private static final String NAME_COUNT = " Count";
		private static final String NAME_MAX_TIME = " Max. Time";
		private static final String NAME_MIN_TIME = " Min. Time";
		private static final String NAME_AVG_TIME = " Avg. Time";

		private static final int OFFSET_COUNT = 0;
		private static final int OFFSET_MAX_TIME = 1;
		private static final int OFFSET_MIN_TIME = 2;
		private static final int OFFSET_AVG_TIME = 3;

		public static final int ENTRY_COUNT = 4;

		private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance();

		// Attributes
		private long m_count;
		private long m_max;
		private long m_min;
		private double m_avg;

		private Lock m_lock;

		// Constructors
		/**
		 * Creates an instance of OperationStatistic
		 */
		public OperationStatistic() {
			m_count = 0;
			m_max = 0;
			m_min = Long.MAX_VALUE;
			m_avg = 0;

			m_lock = new ReentrantLock(false);
		}

		// Methods
		/**
		 * Journalizes an access
		 * @param p_time
		 *            the time of the operation
		 */
		public void update(final long p_time) {
			m_lock.lock();

			m_count++;
			if (p_time > m_max) {
				m_max = p_time;
			}
			if (p_time < m_min) {
				m_min = p_time;
			}
			if (m_count == 1) {
				m_avg = p_time;
			} else {
				m_avg = ((m_avg * (m_count - 1)) + p_time) / m_count;
			}

			m_lock.unlock();
		}

		/**
		 * Adds the operation statistic to a list
		 * @param p_list
		 *            the list
		 * @param p_prefix
		 *            the operation prefix
		 * @param p_start
		 *            the start offset
		 */
		public void addValues(final List<StatisticEntry> p_list, final String p_prefix, final int p_start) {
			String count;
			String max;
			String min;
			String avg;

			m_lock.lock();

			if (m_count > 0) {
				count = NUMBER_FORMAT.format(m_count);
				max = Tools.readableNanoTime(m_max);
				if (m_min == Long.MAX_VALUE) {
					min = Tools.readableNanoTime(0);
				} else {
					min = Tools.readableNanoTime(m_min);
				}
				avg = Tools.readableNanoTime((long) m_avg);

				p_list.add(new StatisticEntry(p_start + OFFSET_COUNT, p_prefix + NAME_COUNT, count));
				p_list.add(new StatisticEntry(p_start + OFFSET_MAX_TIME, p_prefix + NAME_MAX_TIME, max));
				p_list.add(new StatisticEntry(p_start + OFFSET_MIN_TIME, p_prefix + NAME_MIN_TIME, min));
				p_list.add(new StatisticEntry(p_start + OFFSET_AVG_TIME, p_prefix + NAME_AVG_TIME, avg));
			}

			m_lock.unlock();
		}

	}

	/**
	 * TimerTask for the periodic statistic output
	 * @author klein 26.03.2015
	 */
	private static class StatisticsTask extends TimerTask {

		// Constants
		private static final String SEPERATOR = "--------------------------------------------------";

		// Attributes
		private PrintStream m_stream;

		// Constructors
		/**
		 * Creates an instance of StatisticTask
		 * @param p_stream
		 *            the output stream
		 */
		public StatisticsTask(final PrintStream p_stream) {
			m_stream = p_stream;
		}

		// Methods
		@Override
		public void run() {
			StringBuffer buffer;

			buffer = new StringBuffer();
			buffer.append("\n");
			buffer.append(DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).format(new Date()));
			buffer.append("\n");
			buffer.append("\n");

			m_lock.lock();

			if (m_statistics != null) {
				for (Entry<String, Statistic> entry : m_statistics.entrySet()) {
					buffer.append(entry.getKey() + ":\n");
					buffer.append(SEPERATOR + "\n");
					appendStatistic(buffer, entry.getValue());
					buffer.append(SEPERATOR + "\n");
					buffer.append("\n");
				}
			}

			m_lock.unlock();

			m_stream.println(buffer);
		}

		/**
		 * Appends a statistic to a buffer
		 * @param p_buffer
		 *            the buffer
		 * @param p_statistic
		 *            the statistic
		 */
		private void appendStatistic(final StringBuffer p_buffer, final Statistic p_statistic) {
			List<StatisticEntry> values;
			String name;
			int maxLength;

			values = p_statistic.getValues(m_printDetails);
			Collections.sort(values);
			if (values != null) {
				maxLength = 0;
				for (StatisticEntry entry : values) {
					name = entry.getName();
					if (name.length() > maxLength) {
						maxLength = name.length();
					}
				}

				for (StatisticEntry entry : values) {
					name = entry.getName();

					p_buffer.append(name + ": ");
					for (int i = name.length(); i < maxLength; i++) {
						p_buffer.append(" ");
					}
					p_buffer.append(entry.getValue() + "\n");
				}
			}
		}

	}

}
