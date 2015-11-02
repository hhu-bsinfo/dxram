package de.uniduesseldorf.dxram.test.nothaas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.storage.RawMemory;
import de.uniduesseldorf.dxram.core.chunk.storage.StorageRandomAccessFile;
import de.uniduesseldorf.dxram.core.chunk.storage.StorageUnsafeMemory;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.locks.JNILock;

public class RawMemoryTest 
{
	private RawMemory m_memory = null;
	private int m_numThreads = -1;
	private int m_numOperations = -1;
	private float m_mallocFreeRatio;
	private int m_blockSizeMin = -1;
	private int m_blockSizeMax = -1;
	
	public RawMemoryTest(final long p_memorySize, final long p_segmentSize, 
			final int p_numThreads, final int p_numOperations, final float p_mallocFreeRatio, 
			final int p_blockSizeMin, final int p_blockSizeMax)
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
		
		JNILock.load("/Users/rubbinnexx/Workspace/Uni/DXRAM/workspace/dxram/jni/libJNILock.dylib");
		
		//m_memory = new RawMemory(new StorageUnsafeMemory());
		try {
			File file = new File("rawMemory.dump");
			if (file.exists())
			{
				file.delete();
				file.createNewFile();
			}
			
			m_memory = new RawMemory(new StorageRandomAccessFile(file));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			m_memory.initialize(p_memorySize, p_segmentSize);
		} catch (MemoryException e) {
			e.printStackTrace();
		}
		
		m_numThreads = p_numThreads;
		m_numOperations = p_numOperations;
		m_mallocFreeRatio = p_mallocFreeRatio;
		m_blockSizeMin = p_blockSizeMin;
		m_blockSizeMax = p_blockSizeMax;
	}
	
	public void run()
	{
		ExecutorService executor = Executors.newFixedThreadPool(m_numThreads);
		
		System.out.println("Starting " + m_numThreads + " threads.");
		Vector<Future<?>> submitedTasks = new Vector<Future<?>>();
		for (int i = 0; i < m_numThreads; i++)
		{
			MemoryThread memThread = new MemoryThread(m_memory, m_numOperations, 
					m_mallocFreeRatio, m_blockSizeMin, m_blockSizeMax);
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
		m_memory.printDebugInfos();
		
		executor.shutdown();
	}
	
	public static void main(String[] args) throws MemoryException
	{
		if (args.length < 7)
		{
			System.out.println("Usage: RawMemoryTest <memorySize> <segmentSize> <numThreads> <numOperations> <mallocFreeRatio> <blockSizeMin> <blockSizeMax>");
			return;
		}
		
		long memorySize = Long.parseLong(args[0]);
		long segmentSize = Long.parseLong(args[1]);
		int numThreads = Integer.parseInt(args[2]);
		int numOperations = Integer.parseInt(args[3]);
		float mallocFreeRatio = Float.parseFloat(args[4]);
		int blockSizeMin = Integer.parseInt(args[5]);
		int blockSizeMax = Integer.parseInt(args[6]);
		
		System.out.println("Initializing test...");
		RawMemoryTest test = new RawMemoryTest(memorySize, segmentSize, numThreads, numOperations, mallocFreeRatio, blockSizeMin, blockSizeMax);
		System.out.println("Running test...");
		test.run();
		System.out.println("Test done.");
	}
	
	
	
	public class MemoryThread implements Runnable
	{
		private RawMemory m_memory;
		
		private float m_mallocFreeRatio = 0;
		private int m_numMallocOperations = -1;
		private int m_numFreeOperations = -1;
		private int m_blockSizeMin = -1;
		private int m_blockSizeMax = -1;
		
		private Vector<Long> m_blocksAlloced = new Vector<Long>();
		
		public MemoryThread(RawMemory rawMemory, int numOperations, float mallocFreeRatio, int blockSizeMin, int blockSizeMax)
		{
			assert m_blockSizeMin > 0;
			assert m_blockSizeMax > 0;
			assert m_mallocFreeRatio > 0;
			
			m_memory = rawMemory;
			m_blockSizeMin = blockSizeMin;
			m_blockSizeMax = blockSizeMax;
			m_mallocFreeRatio = mallocFreeRatio;
		
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
			System.out.println("(" + Thread.currentThread().getId() + ")" + this);
			
			while (m_numMallocOperations + m_numFreeOperations > 0)
			{
				// random pick operations, depending on ratio
				if ((Math.random() <= m_mallocFreeRatio || m_numFreeOperations <= 0) && m_numMallocOperations > 0)
				{
					// execute alloc
					int size = 0;
					while (size <= 0)
						size = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin));
					System.out.println(size);
					try {
						long ptr = -1;
						
						ptr = m_memory.malloc(size);
						m_blocksAlloced.add(ptr);
						m_memory.set(ptr, size, (byte) 0xFF);
					} catch (MemoryException e) {
						System.out.println("Malloc try size: " + size);
						printDebug();
						e.printStackTrace();
						return;
					}
					
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
							m_memory.free(memoryPtr);
						} catch (MemoryException e) {
							System.out.println("Free try address: " + memoryPtr);
							printDebug();
							e.printStackTrace();
							return;
						}
						
						m_numFreeOperations--;
					}
				}
			}
		}
		
		public void printDebug()
		{
			System.out.println("mallocOpsLeft " + m_numMallocOperations + ", freeOpsLeft: " + m_numFreeOperations);
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
