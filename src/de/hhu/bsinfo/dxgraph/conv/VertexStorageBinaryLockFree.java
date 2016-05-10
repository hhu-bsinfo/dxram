package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.UnsafeHandler;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by nothaas on 5/10/16.
 */
public class VertexStorageBinaryLockFree implements VertexStorage {

	private static final int MS_BLOCK_TABLE_SIZE = 0x100;
	private static final int MS_BLOCK_SIZE = 0x1000000;
	private static final int MS_ENTRY_SIZE_BYTES = Integer.BYTES + Long.BYTES;
	private static final int MS_BLOCK_SIZE_BYTES = MS_BLOCK_SIZE * MS_ENTRY_SIZE_BYTES;

	private Unsafe m_unsafe;

	private AtomicLong m_vertexCount = new AtomicLong(0);
	private AtomicLong m_edgeCount = new AtomicLong(0);
	private AtomicLong m_totalMemory = new AtomicLong(0);

	private ReentrantLock m_blockTableAllocLock = new ReentrantLock(false);
	private long[] m_blockTable;

	public VertexStorageBinaryLockFree() {
		m_unsafe = UnsafeHandler.getInstance().getUnsafe();

		m_blockTable = new long[MS_BLOCK_TABLE_SIZE];
		for (int i = 0; i < m_blockTable.length; i++) {
			m_blockTable[i] = -1;
		}

		m_totalMemory.addAndGet(MS_BLOCK_TABLE_SIZE * m_blockTable.length + 12);

		VertexStorageBinaryLockFree storage = this;
		Thread t = new Thread() {
			@Override
			public void run() {
				while (true) {
					System.out.println("Memory: " + storage.getTotalMemoryDataStructures() / 1024 / 1024);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.start();
	}

	public long getTotalMemoryDataStructures() {
		return m_totalMemory.get();
	}

	//	public static void main(String[] args) {
	//		VertexStorageBinaryLockFree storage = new VertexStorageBinaryLockFree();
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
	//					for (int i = 0; i < 100000; i++) {
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
	//		System.out.println(storage.getTotalEdgeCount());
	//	}

	@Override
	public long getVertexId(long p_hashValue) {
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
				m_vertexCount.incrementAndGet();
			}
		}

		return p_hashValue;
	}

	@Override
	public void putNeighbour(long p_vertexId, long p_neighbourVertexId) {
		int tableIndex = (int) (p_vertexId / MS_BLOCK_SIZE);
		int tableEntryIndex = (int) (p_vertexId % MS_BLOCK_SIZE);

		long ptrBlock = m_blockTable[tableIndex];
		if (ptrBlock == -1) {
			throw new RuntimeException("Access to block that was not allocated before, should not happen");
		}

		int entryHeader = -1;
		// acquire lock for entry
		while (true) {
			// lock set, wait for free
			entryHeader = m_unsafe.getInt(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES);
			entryHeader &= ~0x80000000; // we need this unlocked
			if (m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, entryHeader,
					entryHeader | 0x80000000)) {
				entryHeader |= 0x80000000;
				break;
			}

			Thread.yield();
		}

		// reallocate to expand array
		int sizeOld = entryHeader & 0x7FFFFFFF;
		int sizeNew = sizeOld + 1;
		long ptrOldArray = m_unsafe.getLong(ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES + 4);
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
		}

		m_unsafe.putLong(ptrNewArray + sizeOld * Long.BYTES, p_neighbourVertexId);

		// expecting old size and lock -> swap with new size (old size + 1) and unlocked
		if (!m_unsafe.compareAndSwapInt(null, ptrBlock + tableEntryIndex * MS_ENTRY_SIZE_BYTES, entryHeader, sizeNew)) {
			throw new RuntimeException("Invalid synchronsation state.");
		}

		m_edgeCount.incrementAndGet();
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
