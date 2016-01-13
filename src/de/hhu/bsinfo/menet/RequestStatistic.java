
package de.hhu.bsinfo.menet;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.utils.Tools;
import de.hhu.bsinfo.utils.StatisticsManager.Statistic;
import de.hhu.bsinfo.utils.StatisticsManager.StatisticEntry;

/**
 * Request-Statistic
 * @author klein 26.03.2015
 */
public final class RequestStatistic implements Statistic {

	// Constants
	private static final String NAME_REQUEST_COUNT = "Request Count";
	private static final String NAME_TIMEOUT_COUNT = "Request Timeouts";
	private static final String NAME_ABORT_COUNT = "Aborted Requests";
	private static final String NAME_TIMES = "RequestTime < ";

	private static final String NAME_PREFIX_OVERALL = "Overall ";

	private static final int OFFSET_REQUEST_COUNT = 1;
	private static final int OFFSET_TIMEOUT_COUNT = 2;
	private static final int OFFSET_ABORT_COUNT = 3;
	private static final int OFFSET_TIMES = 4;
	private static final int OFFSET_REQUEST = 1000000;

	private static final int TIME_FACTOR = 10;
	private static final int MAX_TIME = 10000;
	private static final int MAX_VALUES = MAX_TIME / TIME_FACTOR;

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance();

	// Attributes
	private Map<Long, Long> m_requestMap;
	private long m_requestCount;
	private long m_timeoutCount;
	private long m_abortCount;
	private int[] m_times;
	private Map<Class<? extends AbstractRequest>, RequestValues> m_requestValues;

	private Lock m_lock;

	// Constructors
	/**
	 * Creates an instance of RequestStatistic
	 */
	private RequestStatistic() {
		m_requestMap = new HashMap<>();
		m_requestCount = 0;
		m_timeoutCount = 0;
		m_abortCount = 0;
		m_times = new int[MAX_VALUES];
		m_requestValues = new HashMap<>();

		m_lock = new ReentrantLock(false);
	}

	// Methods
	/**
	 * Get the instance of the NetworkStatistic
	 * @return the instance of the NetworkStatistic
	 */
	public static RequestStatistic getInstance() {
		return Holder.INSTANCE;
	}

	/**
	 * Journalizes a send request
	 * @param p_requestID
	 *            the requestID
	 */
	public void requestSend(final long p_requestID) {
		m_lock.lock();

		m_requestMap.put(p_requestID, System.currentTimeMillis());

		m_lock.unlock();
	}

	/**
	 * Journalizes a received response
	 * @param p_requestID
	 *            the requestID
	 * @param p_class
	 *            the request class
	 */
	public void responseReceived(final long p_requestID, final Class<? extends AbstractRequest> p_class) {
		Long start;
		long time;
		RequestValues values;

		m_lock.lock();

		start = m_requestMap.get(p_requestID);
		if (start != null) {
			m_requestCount++;

			time = System.currentTimeMillis() - start;
			m_times[(int) (time / TIME_FACTOR)]++;

			values = m_requestValues.get(p_class);
			if (values == null) {
				values = new RequestValues();
				m_requestValues.put(p_class, values);
			}
			values.responseReceived(time);
		}

		m_lock.unlock();
	}

	/**
	 * Journalizes an aborted request
	 * @param p_requestID
	 *            the requestID
	 * @param p_class
	 *            the request class
	 */
	public void requestAborted(final long p_requestID, final Class<? extends AbstractRequest> p_class) {
		Long start;
		RequestValues values;

		m_lock.lock();

		start = m_requestMap.remove(p_requestID);
		if (start != null) {
			m_abortCount++;

			values = m_requestValues.get(p_class);
			if (values == null) {
				values = new RequestValues();
				m_requestValues.put(p_class, values);
			}
			values.requestAborted();
		}

		m_lock.unlock();
	}

	/**
	 * Journalizes a request timeout
	 * @param p_requestID
	 *            the requestID
	 * @param p_class
	 *            the request class
	 */
	public void requestTimeout(final long p_requestID, final Class<? extends AbstractRequest> p_class) {
		Long start;
		RequestValues values;

		m_lock.lock();

		start = m_requestMap.remove(p_requestID);
		if (start != null) {
			m_timeoutCount++;

			values = m_requestValues.get(p_class);
			if (values == null) {
				values = new RequestValues();
				m_requestValues.put(p_class, values);
			}
			values.requestTimeout();
		}

		m_lock.unlock();
	}

	@Override
	public List<StatisticEntry> getValues(final boolean p_withDetails) {
		List<StatisticEntry> ret;
		int requests;

		ret = new ArrayList<>();

		m_lock.lock();

		ret.add(new StatisticEntry(OFFSET_REQUEST_COUNT, NAME_PREFIX_OVERALL + NAME_REQUEST_COUNT, NUMBER_FORMAT.format(m_requestCount)));
		ret.add(new StatisticEntry(OFFSET_TIMEOUT_COUNT, NAME_PREFIX_OVERALL + NAME_TIMEOUT_COUNT, NUMBER_FORMAT.format(m_timeoutCount)));
		ret.add(new StatisticEntry(OFFSET_ABORT_COUNT, NAME_PREFIX_OVERALL + NAME_ABORT_COUNT, NUMBER_FORMAT.format(m_abortCount)));

		for (int i = 0; i < MAX_VALUES; i++) {
			if (m_times[i] > 0) {
				ret.add(new StatisticEntry(OFFSET_TIMES + i, NAME_PREFIX_OVERALL + NAME_TIMES + Tools.readableTime((i + 1) * TIME_FACTOR), NUMBER_FORMAT
						.format(m_times[i])));
			}
		}

		if (p_withDetails) {
			requests = 1;
			for (Entry<Class<? extends AbstractRequest>, RequestValues> entry : m_requestValues.entrySet()) {
				entry.getValue().addValues(ret, "(" + entry.getKey().getSimpleName() + ") ", OFFSET_REQUEST * requests);

				requests++;
			}
		}

		m_lock.unlock();

		return ret;
	}

	// Classes
	/**
	 * Wrapper class for request attributes
	 * @author klein 26.03.2015
	 */
	private final class RequestValues {

		// Attributes
		private long m_requestCount;
		private long m_timeouts;
		private long m_aborts;
		private long[] m_times;

		// Constructors
		/**
		 * Creates an instance of RequestValues
		 */
		RequestValues() {
			m_requestCount = 0;
			m_timeouts = 0;
			m_aborts = 0;
			m_times = new long[MAX_VALUES];
		}

		// Methods
		/**
		 * Journalizes a received response
		 * @param p_time
		 *            the time between request and response
		 */
		public void responseReceived(final long p_time) {
			m_requestCount++;
			m_times[(int) p_time / TIME_FACTOR]++;
		}

		/**
		 * Journalizes an aborted request
		 */
		public void requestAborted() {
			m_aborts++;
		}

		/**
		 * Journalizes a request timeout
		 */
		public void requestTimeout() {
			m_timeouts++;
		}

		/**
		 * Adds statistic values to a list
		 * @param p_list
		 *            the list
		 * @param p_prefix
		 *            the prefix for the statistic
		 * @param p_start
		 *            the start offset
		 */
		public void addValues(final List<StatisticEntry> p_list, final String p_prefix, final int p_start) {
			p_list.add(new StatisticEntry(p_start + OFFSET_REQUEST_COUNT, p_prefix + NAME_REQUEST_COUNT, NUMBER_FORMAT.format(m_requestCount)));
			p_list.add(new StatisticEntry(p_start + OFFSET_TIMEOUT_COUNT, p_prefix + NAME_TIMEOUT_COUNT, NUMBER_FORMAT.format(m_timeouts)));
			p_list.add(new StatisticEntry(p_start + OFFSET_ABORT_COUNT, p_prefix + NAME_ABORT_COUNT, NUMBER_FORMAT.format(m_aborts)));

			for (int i = 0; i < MAX_VALUES; i++) {
				if (m_times[i] > 0) {
					p_list.add(new StatisticEntry(p_start + OFFSET_TIMES + i, p_prefix + NAME_TIMES + Tools.readableTime((i + 1) * TIME_FACTOR), NUMBER_FORMAT
							.format(m_times[i])));
				}
			}
		}

	}

	/**
	 * Implements the SingeltonPattern for RequestStatistic
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static final class Holder {

		// Constants
		private static final RequestStatistic INSTANCE = new RequestStatistic();

		// Constructors
		/**
		 * Creates an instance of Holder
		 */
		private Holder() {}

	}

}
