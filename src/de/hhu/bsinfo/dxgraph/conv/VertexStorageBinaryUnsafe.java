package de.hhu.bsinfo.dxgraph.conv;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.utils.UnsafeHandler;
import sun.misc.Unsafe;

/**
 * Space efficient and multi threaded optimized storage for vertex data.
 * This does not re-map/re-hash the input data. Instead it expects the input
 * data to have sequential IDs already and not being sparse with ID gaps.
 * Data is stored using Java's Unsafe class to allow manual memory management
 * and space efficient data structures as well as CAS operations for low overhead
 * synchronisation.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.05.16
 */
class VertexStorageBinaryUnsafe implements VertexStorage {

	private static final int MS_BLOCK_TABLE_SIZE = 0x100;
	private static final int MS_BLOCK_SIZE = 0x1000000;
	private static final int MS_ENTRY_SIZE_BYTES = Integer.BYTES + Long.BYTES;
	private static final int MS_BLOCK_SIZE_BYTES = MS_BLOCK_SIZE * MS_ENTRY_SIZE_BYTES;

	//private int m_vertexIdOffset;

	private Unsafe m_unsafe;

	private AtomicLong m_vertexCount = new AtomicLong(0);
	private AtomicLong m_edgeCount = new AtomicLong(0);
	private AtomicLong m_totalMemory = new AtomicLong(0);

	private ReentrantLock m_blockTableAllocLock = new ReentrantLock(false);
	private long[] m_blockTable;

	/**
	 * Constructor
	 *
	 * @param p_vertexIdOffset Offset to add to every vertex id.
	 */
	VertexStorageBinaryUnsafe(final int p_vertexIdOffset) {
		//m_vertexIdOffset = p_vertexIdOffset;

		m_unsafe = UnsafeHandler.getInstance().getUnsafe();

		m_blockTable = new long[MS_BLOCK_TABLE_SIZE];
		for (int i = 0; i < m_blockTable.length; i++) {
			m_blockTable[i] = -1;
		}

		m_totalMemory.addAndGet(MS_BLOCK_TABLE_SIZE * m_blockTable.length + 12);

		VertexStorageBinaryUnsafe storage = this;
		Thread t = new Thread() {
			@Override
			public void run() {
				while (true) {
					System.out.println(
							"MemoryConsumptionStorage (MB): " + storage.getTotalMemoryDataStructures() / 1024 / 1024);
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.start();
	}

	@Override
	public long getTotalMemoryDataStructures() {
		return m_totalMemory.get();
	}

	//	public static void main(String[] args) {
	//		VertexStorageBinaryUnsafe storage = new VertexStorageBinaryUnsafe();
	//
	//		Thread[] threads = new Thread[12];
	//
	//		for (int i = 0; i < threads.length; i++) {
	//			threads[i] = new Thread() {
	//				@Override
	//				public void run() {
	//
	//					long vertex = storage.getVertexId(1);
	//
	//					for (int i = 0; i < 10000; i++) {
	//						storage.putNeighbour(vertex, i);
	//					}
	//
	//					System.out.println("Done " + Thread.currentThread().getId());
	//				}
	//			};
	//			threads[i].start();
	//		}
	//
	//		for (int i = 0; i < threads.length; i++) {
	//			try {
	//				threads[i].join();
	//			} catch (InterruptedException e) {
	//				e.printStackTrace();
	//			}
	//		}
	//
	//		System.out.println(storage.getNeighbours(1, new long[0]));
	//
	//		System.out.println(storage.getTotalEdgeCount());
	//	}

	@Override
	public long getVertexId(final long p_hashValue) {
		int tableIndex = (int) (p_hashValue / MS_BLOCK_SIZE);

		if (tableIndex >= MS_BLOCK_TABLE_SIZE) {
			throw new RuntimeException("Table not big enough to hold vertex id " + p_hashValue);
		}

		// allocate new blocks when getting indexed
		long ptrBlock = m_blockTable[tableIndex];
		if (ptrBlock == -1) {
			m_blockTableAllocLock.lock();
			if (m_blockTable[tableIndex] == -1) {
				long ptr = m_unsafe.allocateMemory(MS_BLOCK_SIZE_BYTES);
				m_totalMemory.addAndGet(MS_BLOCK_SIZE_BYTES);
				m_unsafe.setMemory(ptr, MS_BLOCK_SIZE_BYTES, (byte) -1);
				m_blockTable[tableIndex] = ptr;
			}
			ptrBlock = m_blockTable[tableIndex];
			m_blockTableAllocLock.unlock();
		}

		int tableEntryIndex = (int) (p_hashValue % MS_BLOCK_SIZE);

		// check if table entry already created, if not: prepare for first usage
		int tableEntryHeader = m_unsafe.getInt(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES);
		if (tableEntryHeader == -1) {
			// create entry but do not allocate memory for edges
			// create header with size 0 and unlocked entry
			// allocation will be execution on neighbor put
			if (m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, -1, 0)) {
				while (true) {
					long count = m_vertexCount.get();
					if (count < p_hashValue) {
						if (m_vertexCount.compareAndSet(count, p_hashValue)) {
							break;
						}
					} else {
						break;
					}
				}
			}
		}

		return p_hashValue;
	}

	@Override
	public void putNeighbour(final long p_vertexId, final long p_neighbourVertexId) {
		//long vertexId = p_vertexId;
		// TODO conflict with graph partition index not knowing there was an offset applied
		// -> have information in ioel file about offset?
		//long neighbourVertexId = p_neighbourVertexId + m_vertexIdOffset;
		//long neighbourVertexId = p_neighbourVertexId;

		// don't add self loops
		if (p_vertexId == p_neighbourVertexId) {
			return;
		}

		int tableIndex = (int) (p_vertexId / MS_BLOCK_SIZE);
		int tableEntryIndex = (int) (p_vertexId % MS_BLOCK_SIZE);

		long ptrBlock = m_blockTable[tableIndex];
		if (ptrBlock == -1) {
			throw new RuntimeException("Access to block that was not allocated before, should not happen");
		}

		int entryHeader;
		// acquire lock for entry
		while (true) {
			// lock set, wait for free
			entryHeader = m_unsafe.getInt(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES);
			// we need this unlocked
			entryHeader &= ~0x80000000;
			if (m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, entryHeader,
					entryHeader | 0x80000000)) {
				entryHeader |= 0x80000000;
				break;
			}

			Thread.yield();
		}

		// first, verify the edge does not exist, yet
		int sizeOld = entryHeader & 0x7FFFFFFF;
		long ptrOldArray = m_unsafe.getLong(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES + 4);
		//		for (int i = 0; i < sizeOld; i++) {
		//			long neighbor = m_unsafe.getLong(ptrOldArray + i * Long.BYTES);
		//			if (neighbor == neighbourVertexId) {
		//				// drop out, neighbor already exists
		//				// expecting old size and lock -> swap with old size and unlocked
		//				if (!m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, entryHeader,
		//						sizeOld)) {
		//					throw new RuntimeException("Invalid synchronsation state.");
		//				}
		//				return;
		//			}
		//		}

		// reallocate to expand array
		int sizeNew = sizeOld + 1;
		long ptrNewArray = m_unsafe.allocateMemory(sizeNew * Long.BYTES);
		m_totalMemory.addAndGet(sizeNew * Long.BYTES);
		if (ptrOldArray != -1) {
			// move previous data
			m_unsafe.copyMemory(ptrOldArray, ptrNewArray, sizeOld * Long.BYTES);

			// swap pointers
			m_unsafe.putLong(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES + 4, ptrNewArray);

			// free old data
			m_unsafe.freeMemory(ptrOldArray);
			m_totalMemory.addAndGet(-sizeOld * Long.BYTES);
		} else {
			// swap invalid poiner for first allocation
			m_unsafe.putLong(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES + 4, ptrNewArray);
		}

		if (p_neighbourVertexId == -1) {
			System.out.println("Invalid neighbour: " + p_neighbourVertexId);
		}

		m_unsafe.putLong(ptrNewArray + sizeOld * Long.BYTES, p_neighbourVertexId);

		// expecting old size and lock -> swap with new size (old size + 1) and unlocked
		if (!m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, entryHeader, sizeNew)) {
			throw new RuntimeException("Invalid synchronsation state.");
		}

		m_edgeCount.incrementAndGet();
	}

	@Override
	public long getNeighbours(final long p_vertexId, final long[] p_buffer) {
		//long vertexId = p_vertexId;

		int tableIndex = (int) (p_vertexId / MS_BLOCK_SIZE);
		int tableEntryIndex = (int) (p_vertexId % MS_BLOCK_SIZE);

		long ptrBlock = m_blockTable[tableIndex];
		if (ptrBlock == -1) {
			throw new RuntimeException("Access to block that was not allocated before, should not happen");
		}

		// we don't have to lock here, because this is executed separately from the putting
		int entryHeader = m_unsafe.getInt(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES);
		if (entryHeader == -1) {
			// invalid entry for vertex id
			return Long.MAX_VALUE;
		}
		int arraySize = entryHeader & 0x7FFFFFFF;

		// is buffer big enough?
		if (p_buffer.length < arraySize) {
			return -arraySize;
		}

		long ptrArray = m_unsafe.getLong(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES + 4);

		for (int i = 0; i < arraySize; i++) {
			p_buffer[i] = m_unsafe.getLong(ptrArray + Long.BYTES * i);
		}

		return arraySize;
	}

	@Override
	public long getTotalVertexCount() {
		return m_vertexCount.get();
	}

	@Override
	public long getTotalEdgeCount() {
		return m_edgeCount.get();
	}
}
