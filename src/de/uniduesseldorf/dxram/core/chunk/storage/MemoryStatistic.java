
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.utils.StatisticsManager.Statistic;
import de.uniduesseldorf.dxram.utils.StatisticsManager.StatisticEntry;
import de.uniduesseldorf.dxram.utils.Tools;

/**
 * Memory-Statistic
 * @author klein 26.03.2015
 */
final class MemoryStatistic implements Statistic {

	// Constants
	private static final String NAME_MALLOC_COUNT = "Allocation Count";
	private static final String NAME_FREE_COUNT = "Free Count";
	private static final String NAME_FREE_MEMORY = "Free Memory";
	private static final String NAME_TABLE_COUNT = "Table Count";

	private static final int POSITION_MALLOC_COUNT = 1;
	private static final int POSITION_FREE_COUNT = 2;
	private static final int POSITION_FREE_MEMORY = 3;
	private static final int POSITION_TABLE_COUNT = 4;

	private static final int ENTRY_COUNT = 4;
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance();

	// Attributes
	private long m_mallocCount;
	private long m_freeCount;
	private long m_freeMemory;

	private long m_tableCount;

	private Lock m_lock;

	// Constructors
	/**
	 * Create a instance of MemoryStatistic
	 */
	private MemoryStatistic() {
		m_lock = new ReentrantLock(false);
	}

	// Methods
	/**
	 * Get the instance of the MemoryStatistic
	 * @return the instance of the MemoryStatistic
	 */
	public static MemoryStatistic getInstance() {
		return Holder.INSTANCE;
	}

	/**
	 * Initialize the size of the memory
	 * @param p_size
	 *            the size of the memory
	 */
	public void initMemory(final long p_size) {
		m_lock.lock();

		m_freeMemory = p_size;

		m_lock.unlock();
	}

	/**
	 * Journalizes a malloc call
	 * @param p_size
	 *            the size of the malloc
	 */
	public void malloc(final long p_size) {
		m_lock.lock();

		m_mallocCount++;
		m_freeMemory -= p_size;

		m_lock.unlock();
	}

	/**
	 * Journalizes a free call
	 * @param p_size
	 *            the size of the malloc
	 */
	public void free(final long p_size) {
		m_lock.lock();

		m_freeCount++;
		m_freeMemory += p_size;

		m_lock.unlock();
	}

	/**
	 * Journalizes a CIDTable creation
	 */
	public void newCIDTable() {
		m_lock.lock();

		m_tableCount++;

		m_lock.unlock();
	}

	@Override
	public List<StatisticEntry> getValues(final boolean p_withDetails) {
		List<StatisticEntry> ret;

		ret = new ArrayList<>(ENTRY_COUNT);

		m_lock.lock();

		ret.add(new StatisticEntry(POSITION_MALLOC_COUNT, NAME_MALLOC_COUNT, NUMBER_FORMAT.format(m_mallocCount)));
		ret.add(new StatisticEntry(POSITION_FREE_COUNT, NAME_FREE_COUNT, NUMBER_FORMAT.format(m_freeCount)));
		ret.add(new StatisticEntry(POSITION_FREE_MEMORY, NAME_FREE_MEMORY, Tools.readableSize(m_freeMemory)));
		ret.add(new StatisticEntry(POSITION_TABLE_COUNT, NAME_TABLE_COUNT, NUMBER_FORMAT.format(m_tableCount)));

		m_lock.unlock();

		return ret;
	}

	// Classes
	/**
	 * Implements the SingeltonPattern for MemoryStatistic
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static final class Holder {

		// Constants
		private static final MemoryStatistic INSTANCE = new MemoryStatistic();

		// Constructors
		/**
		 * Creates an instance of Holder
		 */
		private Holder() {}

	}

}
