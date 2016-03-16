package de.hhu.bsinfo.soh;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.hhu.bsinfo.utils.locks.JNILock;
import sun.misc.Lock;

public class SmallObjectHeapTest 
{
	private SmallObjectHeap m_memory = null;
	private int m_numThreads = -1;
	private int m_numOperations = -1;
	private float m_mallocFreeRatio;
	private int m_blockSizeMin = -1;
	private int m_blockSizeMax = -1;
	private boolean m_debugPrint = false;
	
	public SmallObjectHeapTest(final long p_memorySize, final long p_segmentSize, 
			final int p_numThreads, final int p_numOperations, final float p_mallocFreeRatio, 
			final int p_blockSizeMin, final int p_blockSizeMax, final boolean p_debugPrint)
	{
		assert p_memorySize > 0;
		assert p_segmentSize > 0;
		assert p_segmentSize <= p_memorySize;
		assert p_numThreads > 0;
		assert p_numOperations > 0;
		assert p_mallocFreeRatio >= 0.5;
		assert p_blockSizeMin > 0;
		assert p_blockSizeMax > 0;
		assert p_blockSizeMax > p_blockSizeMin;
		
		//JNILock.load("/Users/rubbinnexx/Workspace/Uni/DXRAM/workspace/dxram/jni/libJNILock.dylib");
		JNILock.load("/home/nothaas/Workspace/workspace_dxram/dxram/jni/libJNILock.so");
		
		//m_memory = new SmallObjectHeap(new StorageUnsafeMemory());
		try {
			File file = new File("rawMemory.dump");
			if (file.exists())
			{
				file.delete();
				file.createNewFile();
			}
			
			//m_memory = new SmallObjectHeap(new StorageRandomAccessFile(file));
			m_memory = new SmallObjectHeap(new StorageUnsafeMemory());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		if(m_memory.initialize(p_memorySize, p_segmentSize) == -1)
		{
			System.out.println("Initializing memory failed.");
		}
		
		m_numThreads = p_numThreads;
		m_numOperations = p_numOperations;
		m_mallocFreeRatio = p_mallocFreeRatio;
		m_blockSizeMin = p_blockSizeMin;
		m_blockSizeMax = p_blockSizeMax;
		m_debugPrint = p_debugPrint;
	}
	
	public void run()
	{
		ExecutorService executor = Executors.newFixedThreadPool(m_numThreads);
		
		System.out.println(m_memory);
		
		System.out.println("Starting " + m_numThreads + " threads.");
		Vector<Future<?>> submitedTasks = new Vector<Future<?>>();
		for (int i = 0; i < m_numThreads; i++)
		{
			MemoryThread memThread = new MemoryThread(m_memory, m_numOperations, 
					m_mallocFreeRatio, m_blockSizeMin, m_blockSizeMax, m_debugPrint);
			submitedTasks.add(executor.submit(memThread));
		}
		
		System.out.println("Waiting for workers to finish...");
		
		for (Future<?> future : submitedTasks)
		{
			try
			{
				future.get();
			}
			catch (final ExecutionException | InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	
		System.out.println("All workers finished.");
		System.out.println("Final memory status:\n" + m_memory);
		
		HeapWalker.Results results = HeapWalker.walk(m_memory);
		System.out.println(results);
		HeapIntegrityChecker.check(results);
		
		executor.shutdown();
	}
	
	public static void main(String[] args)
	{
		if (args.length < 8)
		{
			System.out.println("Usage: RawMemoryTest <memorySize> <segmentSize> <numThreads> <numOperations> <mallocFreeRatio> <blockSizeMin> <blockSizeMax> <debugPrint>");
			return;
		}
		
		long memorySize = Long.parseLong(args[0]);
		long segmentSize = Long.parseLong(args[1]);
		int numThreads = Integer.parseInt(args[2]);
		int numOperations = Integer.parseInt(args[3]);
		float mallocFreeRatio = Float.parseFloat(args[4]);
		int blockSizeMin = Integer.parseInt(args[5]);
		int blockSizeMax = Integer.parseInt(args[6]);
		boolean debugPrint = Boolean.parseBoolean(args[7]);
		
		System.out.println("Initializing RawMemory test...");
		SmallObjectHeapTest test = new SmallObjectHeapTest(memorySize, segmentSize, numThreads, numOperations, mallocFreeRatio, blockSizeMin, blockSizeMax, debugPrint);
		System.out.println("Running test...");
		test.run();
		System.out.println("Test done.");
	}
	
	private static final Lock m_lock = new Lock();
	
	public class MemoryThread implements Runnable
	{
		private SmallObjectHeap m_memory;
		
		private float m_mallocFreeRatio = 0;
		private int m_numMallocOperations = -1;
		private int m_numFreeOperations = -1;
		private int m_blockSizeMin = -1;
		private int m_blockSizeMax = -1;
		private boolean m_debugPrint = false;
		
		
		
		private Vector<Long> m_blocksAlloced = new Vector<Long>();
		
		public MemoryThread(SmallObjectHeap rawMemory, int numOperations, float mallocFreeRatio, int blockSizeMin, int blockSizeMax, boolean p_debugPrint)
		{
			assert m_blockSizeMin > 0;
			assert m_blockSizeMax > 0;
			assert m_mallocFreeRatio > 0;
			
			m_memory = rawMemory;
			m_blockSizeMin = blockSizeMin;
			m_blockSizeMax = blockSizeMax;
			m_mallocFreeRatio = mallocFreeRatio;
			m_debugPrint = p_debugPrint;
		
			if (m_blockSizeMax < m_blockSizeMin)
				m_blockSizeMax = m_blockSizeMin;
			
			if (mallocFreeRatio < 0.5)
			{
				m_numMallocOperations = numOperations / 2;
				m_numFreeOperations = m_numMallocOperations;
			}
			else
			{
				m_numMallocOperations = (int) (numOperations * mallocFreeRatio);
				m_numFreeOperations = numOperations - m_numMallocOperations;
			}
		}

		@Override
		public void run() 
		{
			System.out.println("(" + Thread.currentThread().getId() + ") " + this);
			
			while (m_numMallocOperations + m_numFreeOperations > 0)
			{
				// random pick operations, depending on ratio
				if ((Math.random() <= m_mallocFreeRatio || m_numFreeOperations <= 0) && m_numMallocOperations > 0)
				{
					// execute alloc
					int size = 0;
					while (size <= 0)
						size = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin));
	
					long ptr = -1;
					
					try {
						m_lock.lock();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					ptr = m_memory.malloc(size);
					m_lock.unlock();
					if (m_debugPrint)
						System.out.println(">>> Allocated " + size + ":\n" + m_memory);
					m_blocksAlloced.add(ptr);
					m_memory.set(ptr, size, (byte) 0xFF);					
					
					m_numMallocOperations--;
				}
				else if (m_numFreeOperations > 0)
				{
					// execute free if blocks allocated
					if (!m_blocksAlloced.isEmpty())
					{
						Long memoryPtr = m_blocksAlloced.firstElement();
						m_blocksAlloced.remove(0);
						
						try {
							m_lock.lock();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						m_memory.free(memoryPtr);
						m_lock.unlock();
						if (m_debugPrint)
							System.out.println(">>> Freed " + memoryPtr + ":\n" + m_memory);
						
						m_numFreeOperations--;
					}
				}
			}
		}
		
		public void printDebug()
		{
			System.out.println(this);
		}
		
		@Override
		public String toString()
		{
			return "MemoryThread: mallocOperations " + m_numMallocOperations +
					", freeOperations: " + m_numFreeOperations + 
					",  blockSizeMin: " + m_blockSizeMin +
					", blockSizeMax: " + m_blockSizeMax;
		}
	}
}
