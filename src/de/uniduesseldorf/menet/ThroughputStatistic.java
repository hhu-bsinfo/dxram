
package de.uniduesseldorf.menet;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.Tools;
import de.uniduesseldorf.utils.StatisticsManager.Statistic;
import de.uniduesseldorf.utils.StatisticsManager.StatisticEntry;

/**
 * Network Throughput-Statistic
 * @author klein 26.03.2015
 */
public final class ThroughputStatistic implements Statistic {

	// Constants
	private static final String NAME_INCOMING_BYTES = "Incoming Bytes";
	private static final String NAME_INCOMING_BYTES_PER_SECOND = "Incoming Bytes / Second";
	private static final String NAME_INCOMING_INTERVAL = "Incoming Interval ";
	private static final String NAME_INCOMING_INTERVAL_AVG = "Incoming Interval Avg.";
	private static final String NAME_OUTGOING_BYTES = "Outgoing Bytes";
	private static final String NAME_OUTGOING_BYTES_PER_SECOND = "Outgoing Bytes / Second";
	private static final String NAME_OUTGOING_INTERVAL = "Outgoing Interval ";
	private static final String NAME_OUTGOING_INTERVAL_AVG = "Outgoing Interval Avg.";

	private static final int POSITION_INCOMING_BYTES = 1;
	private static final int POSITION_INCOMING_BYTES_PER_SECOND = 2;
	private static final int POSITION_OUTGOING_BYTES = 3;
	private static final int POSITION_OUTGOING_BYTES_PER_SECOND = 4;
	private static final int POSITION_INCOMING_INTERVAL = 100000000;
	private static final int POSITION_OUTGOING_INTERVAL = 200000000;

	private static final int THROUGHPUT_INTERVAL = 60;
	private static final int MAX_VALUES = 60;

	// Attributes
	private long m_incomingBytesExtern;
	private long m_incomingSumExtern;
	private long[] m_incomingValuesExtern;

	private long m_outgoingBytesExtern;
	private long m_outgoingSumExtern;
	private long[] m_outgoingValuesExtern;

	private long m_started;
	private int m_startValue;
	private int m_endValue;

	private Lock m_lock;

	// Constructors
	/**
	 * Creates an instance of ThroughputStatistic
	 */
	private ThroughputStatistic() {
		m_incomingBytesExtern = 0;
		m_incomingSumExtern = 0;
		m_incomingValuesExtern = new long[MAX_VALUES];

		m_outgoingBytesExtern = 0;
		m_outgoingSumExtern = 0;
		m_outgoingValuesExtern = new long[MAX_VALUES];

		m_started = 0;
		m_startValue = 0;
		m_endValue = 0;
		m_lock = new ReentrantLock(false);
	}

	// Methods
	/**
	 * Get the instance of the NetworkStatistic
	 * @return the instance of the NetworkStatistic
	 */
	public static ThroughputStatistic getInstance() {
		return Holder.INSTANCE;
	}

	/**
	 * Journalizes incoming bytes
	 * @param p_bytes
	 *            the number of bytes
	 */
	public void incomingExtern(final int p_bytes) {
		Contract.check(p_bytes >= 0, "invalid bytes value");

		m_lock.lock();

		if (m_started == 0) {
			startTimer();
			m_started = System.currentTimeMillis() / 1000;
		}

		m_incomingBytesExtern += p_bytes;

		m_lock.unlock();
	}

	/**
	 * Journalizes outgoing bytes
	 * @param p_bytes
	 *            the number of bytes
	 */
	public void outgoingExtern(final int p_bytes) {
		Contract.check(p_bytes >= 0, "invalid bytes value");

		m_lock.lock();

		if (m_started == 0) {
			startTimer();
			m_started = System.currentTimeMillis() / 1000;
		}

		m_outgoingBytesExtern += p_bytes;

		m_lock.unlock();
	}

	/**
	 * Starts the timer task
	 */
	private void startTimer() {
		new Timer("NetworkStatistic", true).scheduleAtFixedRate(new ThroughputTask(), 0, THROUGHPUT_INTERVAL * 1000);
	}

	@Override
	public List<StatisticEntry> getValues(final boolean p_withDetails) {
		List<StatisticEntry> ret;
		long time;
		int interval;
		double incomingAvg;
		double outgoingAvg;

		ret = new ArrayList<>();

		m_lock.lock();

		time = System.currentTimeMillis() / 1000 - m_started;

		if (time == 0) {
			ret.add(new StatisticEntry(POSITION_INCOMING_BYTES, NAME_INCOMING_BYTES, Tools.readableSize(0)));
			ret.add(new StatisticEntry(POSITION_INCOMING_BYTES_PER_SECOND, NAME_INCOMING_BYTES_PER_SECOND, Tools.readableSize(0)));

			ret.add(new StatisticEntry(POSITION_OUTGOING_BYTES, NAME_OUTGOING_BYTES, Tools.readableSize(0)));
			ret.add(new StatisticEntry(POSITION_OUTGOING_BYTES_PER_SECOND, NAME_OUTGOING_BYTES_PER_SECOND, Tools.readableSize(0)));
		} else {
			ret.add(new StatisticEntry(POSITION_INCOMING_BYTES, NAME_INCOMING_BYTES, Tools.readableSize(m_incomingSumExtern)));
			ret.add(new StatisticEntry(POSITION_INCOMING_BYTES_PER_SECOND, NAME_INCOMING_BYTES_PER_SECOND, Tools.readableSize(m_incomingSumExtern / time)));

			ret.add(new StatisticEntry(POSITION_OUTGOING_BYTES, NAME_OUTGOING_BYTES, Tools.readableSize(m_outgoingSumExtern)));
			ret.add(new StatisticEntry(POSITION_OUTGOING_BYTES_PER_SECOND, NAME_OUTGOING_BYTES_PER_SECOND, Tools.readableSize(m_outgoingSumExtern / time)));

			if (p_withDetails && m_endValue != -1) {
				interval = 1;
				incomingAvg = 0;
				outgoingAvg = 0;
				for (int i = m_startValue; i != m_endValue; i = (i + 1) % MAX_VALUES) {
					incomingAvg += m_incomingValuesExtern[i];
					ret.add(new StatisticEntry(POSITION_INCOMING_INTERVAL + interval, NAME_INCOMING_INTERVAL + interval, Tools
							.readableSize(m_incomingValuesExtern[i]) + " / s"));

					outgoingAvg += m_outgoingValuesExtern[i];
					ret.add(new StatisticEntry(POSITION_OUTGOING_INTERVAL + interval, NAME_OUTGOING_INTERVAL + interval, Tools
							.readableSize(m_outgoingValuesExtern[i]) + " / s"));

					interval++;
				}
				if (interval > 1) {
					ret.add(new StatisticEntry(POSITION_INCOMING_INTERVAL + interval + 1, NAME_INCOMING_INTERVAL_AVG, Tools.readableSize((long) incomingAvg
							/ (interval - 1))));

					ret.add(new StatisticEntry(POSITION_OUTGOING_INTERVAL + interval + 1, NAME_OUTGOING_INTERVAL_AVG, Tools.readableSize((long) outgoingAvg
							/ (interval - 1))));
				}
			}
		}

		m_lock.unlock();

		return ret;
	}

	// Classes
	/**
	 * TimerTask for interval measurements
	 * @author klein 26.03.2015
	 */
	private final class ThroughputTask extends TimerTask {

		// Attributes
		private long m_startTime;

		// Constructors
		/**
		 * Creates an instance of ThroughputTask
		 */
		ThroughputTask() {
			m_startTime = 0;
			m_startValue = 0;
			m_endValue = -1;
		}

		// Methods
		@Override
		public void run() {
			if (m_startTime == 0) {
				m_startTime = System.currentTimeMillis();
			} else {
				m_lock.lock();

				if (m_endValue == -1) {
					m_endValue = (m_endValue + 1) % MAX_VALUES;
				} else {
					m_endValue = (m_endValue + 1) % MAX_VALUES;
					if (m_startValue == m_endValue) {
						m_startValue = (m_startValue + 1) % MAX_VALUES;
					}
				}

				m_incomingValuesExtern[m_endValue] = m_incomingBytesExtern / THROUGHPUT_INTERVAL;
				m_outgoingValuesExtern[m_endValue] = m_outgoingBytesExtern / THROUGHPUT_INTERVAL;

				m_incomingSumExtern += m_incomingBytesExtern;
				m_outgoingSumExtern += m_outgoingBytesExtern;

				m_incomingBytesExtern = 0;
				m_outgoingBytesExtern = 0;

				m_lock.unlock();
			}
		}
	}

	/**
	 * Implements the SingeltonPattern for NetworkStatistic
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static final class Holder {

		// Constants
		private static final ThroughputStatistic INSTANCE = new ThroughputStatistic();

		// Constructors
		/**
		 * Creates an instance of Holder
		 */
		private Holder() {}

	}

}
