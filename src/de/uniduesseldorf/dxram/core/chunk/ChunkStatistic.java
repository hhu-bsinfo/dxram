
package de.uniduesseldorf.dxram.core.chunk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.utils.StatisticsManager.OperationStatistic;
import de.uniduesseldorf.dxram.utils.StatisticsManager.Statistic;
import de.uniduesseldorf.dxram.utils.StatisticsManager.StatisticEntry;

/**
 * Chunk-Statistic
 * @author klein 26.03.2015
 */
final class ChunkStatistic implements Statistic {

	// Constants
	private static final String PREFIX_CREATE = "Create";
	private static final String PREFIX_MULTI_CREATE = "MultiCreate";
	private static final String PREFIX_GET = "Get";
	private static final String PREFIX_MULTI_GET = "MultiGet";
	private static final String PREFIX_GET_ASYNC = "Get Async";
	private static final String PREFIX_PUT = "Put";
	private static final String PREFIX_REMOVE = "Remove";
	private static final String PREFIX_LOCK = "Lock";
	private static final String PREFIX_UNLOCK = "Unlock";

	private static final String PREFIX_INCOMING_GET = "[Incoming] Get";
	private static final String PREFIX_INCOMING_MULTI_GET = "[Incoming] MultiGet";
	private static final String PREFIX_INCOMING_PUT = "[Incoming] Put";
	private static final String PREFIX_INCOMING_REMOVE = "[Incoming] Remove";
	private static final String PREFIX_INCOMING_LOCK = "[Incoming] Lock";
	private static final String PREFIX_INCOMING_UNLOCK = "[Incoming] Unlock";
	private static final String PREFIX_INCOMING_COMMAND = "[Incoming] Command";

	private static final int POSITION_CREATE = 1;
	private static final int POSITION_MULTI_CREATE = 2;
	private static final int POSITION_GET = 3;
	private static final int POSITION_MULTI_GET = 4;
	private static final int POSITION_GET_ASYNC = 5;
	private static final int POSITION_PUT = 6;
	private static final int POSITION_REMOVE = 7;
	private static final int POSITION_LOCK = 8;
	private static final int POSITION_UNLOCK = 9;

	private static final int POSITION_INCOMING_GET = 10;
	private static final int POSITION_INCOMING_MULTI_GET = 11;
	private static final int POSITION_INCOMING_PUT = 12;
	private static final int POSITION_INCOMING_REMOVE = 13;
	private static final int POSITION_INCOMING_LOCK = 14;
	private static final int POSITION_INCOMING_UNLOCK = 15;
	private static final int POSITION_INCOMING_COMMAND = 16;

	private static final int START_FACTOR = 10;

	// Constructors
	/**
	 * Creates an instance of ChunkStatistic
	 */
	private ChunkStatistic() {}

	// Methods
	/**
	 * Get the instance of the ChunkStatistic
	 * @return the instance of the ChunkStatistic
	 */
	public static ChunkStatistic getInstance() {
		return Holder.INSTANCE;
	}

	@Override
	public List<StatisticEntry> getValues(final boolean p_withDetails) {
		List<StatisticEntry> ret;

		ret = new ArrayList<>(Operation.values().length * OperationStatistic.ENTRY_COUNT);

		for (Operation operation : Operation.values()) {
			operation.addValues(ret);
		}

		return ret;
	}

	// Classes
	/**
	 * Represents the Chunk Operations
	 * @author klein 26.03.2015
	 */
	static enum Operation {
		CREATE(PREFIX_CREATE, POSITION_CREATE),
		MULTI_CREATE(PREFIX_MULTI_CREATE, POSITION_MULTI_CREATE),
		GET(PREFIX_GET, POSITION_GET),
		MULTI_GET(PREFIX_MULTI_GET, POSITION_MULTI_GET),
		GET_ASYNC(PREFIX_GET_ASYNC, POSITION_GET_ASYNC),
		PUT(PREFIX_PUT, POSITION_PUT),
		REMOVE(PREFIX_REMOVE, POSITION_REMOVE),
		LOCK(PREFIX_LOCK, POSITION_LOCK),
		UNLOCK(PREFIX_UNLOCK, POSITION_UNLOCK),
		INCOMING_GET(PREFIX_INCOMING_GET, POSITION_INCOMING_GET),
		INCOMING_MULTI_GET(PREFIX_INCOMING_MULTI_GET, POSITION_INCOMING_MULTI_GET),
		INCOMING_PUT(PREFIX_INCOMING_PUT, POSITION_INCOMING_PUT),
		INCOMING_REMOVE(PREFIX_INCOMING_REMOVE, POSITION_INCOMING_REMOVE),
		INCOMING_LOCK(PREFIX_INCOMING_LOCK, POSITION_INCOMING_LOCK),
		INCOMING_UNLOCK(PREFIX_INCOMING_UNLOCK, POSITION_INCOMING_UNLOCK),
		INCOMING_COMMAND(PREFIX_INCOMING_COMMAND, POSITION_INCOMING_COMMAND);

		// Attributes
		private final String m_prefix;
		private final int m_position;
		private final OperationStatistic m_statistic;

		private final Map<Long, Long> m_accessMap;
		private final Lock m_lock;

		// Constructors
		/**
		 * Creates an instance of Operation
		 * @param p_prefix
		 *            the prefix
		 * @param p_position
		 *            the position
		 */
		Operation(final String p_prefix, final int p_position) {
			m_prefix = p_prefix;
			m_position = p_position;
			m_statistic = new OperationStatistic();

			m_accessMap = new HashMap<>();
			m_lock = new ReentrantLock(false);
		}

		// Methods
		/**
		 * Signals a method entry
		 */
		public void enter() {
			m_lock.lock();

			m_accessMap.put(Thread.currentThread().getId(), System.nanoTime());

			m_lock.unlock();
		}

		/**
		 * Signals a method exit
		 */
		public void leave() {
			Long start;

			m_lock.lock();

			start = m_accessMap.remove(Thread.currentThread().getId());
			if (start != null) {
				m_statistic.update(System.nanoTime() - start);
			}

			m_lock.unlock();
		}

		/**
		 * Adds the operation statistic
		 * @param p_list
		 *            the list where to add the statistic
		 */
		private void addValues(final List<StatisticEntry> p_list) {
			m_statistic.addValues(p_list, m_prefix, m_position * START_FACTOR);
		}

	}

	/**
	 * Implements the SingeltonPattern for ChunkStatistic
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static final class Holder {

		// Constants
		private static final ChunkStatistic INSTANCE = new ChunkStatistic();

		// Constructors
		/**
		 * Creates an instance of Holder
		 */
		private Holder() {}

	}

}
