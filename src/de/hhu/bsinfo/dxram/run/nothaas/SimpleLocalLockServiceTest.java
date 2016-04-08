package de.hhu.bsinfo.dxram.run.nothaas;

import java.util.Random;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Test to verify if the (local) lock service is working.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class SimpleLocalLockServiceTest extends Main {
	
	public static final Argument ARG_CHUNK_SIZE = new Argument("chunkSize", "76", true, "Size of the chunks for allocation");
	public static final Argument ARG_CHUNK_COUNT = new Argument("chunkCount", "10", true, "Number of chunks to allocate");
	public static final Argument ARG_THREAD_COUNT = new Argument("threadCount", "2", true, "Number of threads to run");
	
	private DXRAM m_dxram = null;
	private ChunkService m_chunkService = null;
	private AbstractLockService m_lockService = null;

	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		Main main = new SimpleLocalLockServiceTest();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	public SimpleLocalLockServiceTest()
	{
		super("Simple test to verify if the local lock service is working");
		
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
		m_lockService = m_dxram.getService(AbstractLockService.class);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_CHUNK_SIZE);
		p_arguments.setArgument(ARG_CHUNK_COUNT);
		p_arguments.setArgument(ARG_THREAD_COUNT);
	}

	@Override
	protected int main(ArgumentList p_arguments) {
		final int size = p_arguments.getArgument(ARG_CHUNK_SIZE).getValue(Integer.class);
		final int chunkCount = p_arguments.getArgument(ARG_CHUNK_COUNT).getValue(Integer.class);
		final int threadCount = p_arguments.getArgument(ARG_THREAD_COUNT).getValue(Integer.class);
		
		Chunk[] chunks = new Chunk[chunkCount];
		int[] sizes = new int[chunkCount];
		for (int i = 0; i < sizes.length; i++) {
			sizes[i] = size;
		}
		
		System.out.println("Creating chunks...");
		long[] chunkIDs = m_chunkService.create(sizes);
		
		for (int i = 0; i < chunks.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		System.out.println("Running " + threadCount + " locker theads...");
		Thread[] threads = new Thread[threadCount];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new LockerThread(m_lockService, chunks);
			threads[i].start();
		}
		
		System.out.println("Waiting for locker threads to finish...");
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
			}
		}
		
		System.out.println("Done.");
		return 0;
	}
	
	/**
	 * Thread for execution the lock operations.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 *
	 */
	private static class LockerThread extends Thread {
		
		private Random m_random = new Random();
		private AbstractLockService m_lockService = null;
		private Chunk[] m_chunks = null;
		
		/**
		 * Constructor
		 * @param p_lockService LockService to execute lock operations on.
		 * @param p_chunks List of chunks to lock.
		 */
		public LockerThread(final AbstractLockService p_lockService, final Chunk[] p_chunks) {
			m_lockService = p_lockService;
			m_chunks = p_chunks;
		}
		
		@Override
		public void run() {
			for (Chunk chunk : m_chunks) {
				AbstractLockService.ErrorCode err = m_lockService.lock(true, 1000, chunk);
				if (err != AbstractLockService.ErrorCode.SUCCESS) {
					System.out.println("[Thread " + currentThread().getId() + "] Locking of chunk " + chunk + " failed: " + err);
					continue;
				}
				
				int waitMs = (m_random.nextInt(5) + 1) * 100;
				System.out.println("[Thread " + currentThread().getId() + "] Chunk " + chunk + " locked, waiting " + waitMs + " ms...");
				
				// wait a little before unlocking again
				try {
					Thread.sleep(waitMs);
				} catch (InterruptedException e) {
				}
				
				err = m_lockService.unlock(true, chunk);
				if (err != AbstractLockService.ErrorCode.SUCCESS) {
					System.out.println("[Thread " + currentThread().getId() + "] Unlocking chunk " + chunk + " failed: " + err);
				} else {
					System.out.println("[Thread " + currentThread().getId() + "] Chunk " + chunk + " unlocked.");
				}
			}
		}
	}
}
