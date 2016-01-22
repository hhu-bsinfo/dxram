package de.hhu.bsinfo.dxram.test.nothaas;

import java.util.Random;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.lock.LockService;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;

public class SimpleLocalLockServiceTest extends Main {
	
	public static final Pair<String, Integer> ARG_CHUNK_SIZE = new Pair<String, Integer>("chunkSize", 76);
	public static final Pair<String, Integer> ARG_CHUNK_COUNT = new Pair<String, Integer>("chunkCount", 10);
	public static final Pair<String, Integer> ARG_THREAD_COUNT = new Pair<String, Integer>("threadCount", 2);
	
	private DXRAM m_dxram = null;
	private ChunkService m_chunkService = null;
	private LockService m_lockService = null;

	public static void main(final String[] args) {
		Main main = new SimpleLocalLockServiceTest();
		main.run(args);
	}
	
	public SimpleLocalLockServiceTest()
	{
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
		m_lockService = m_dxram.getService(LockService.class);
	}
	
	@Override
	protected void registerDefaultProgramArguments(MainArguments p_arguments) {
		p_arguments.setArgument(ARG_CHUNK_SIZE);
		p_arguments.setArgument(ARG_CHUNK_COUNT);
		p_arguments.setArgument(ARG_THREAD_COUNT);
	}

	@Override
	protected int main(MainArguments p_arguments) {
		final int size = p_arguments.getArgument(ARG_CHUNK_SIZE);
		final int chunkCount = p_arguments.getArgument(ARG_CHUNK_COUNT);
		final int threadCount = p_arguments.getArgument(ARG_THREAD_COUNT);
		
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
	
	private static class LockerThread extends Thread {
		
		private Random m_random = new Random();
		private LockService m_lockService = null;
		private Chunk[] m_chunks = null;
		
		public LockerThread(final LockService p_lockService, final Chunk[] p_chunks) {
			m_lockService = p_lockService;
			m_chunks = p_chunks;
		}
		
		@Override
		public void run() {
			for (Chunk chunk : m_chunks) {
				LockService.ErrorCode err = m_lockService.lock(true, 1000, chunk);
				if (err != LockService.ErrorCode.SUCCESS) {
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
				if (err != LockService.ErrorCode.SUCCESS) {
					System.out.println("[Thread " + currentThread().getId() + "] Unlocking chunk " + chunk + " failed: " + err);
				} else {
					System.out.println("[Thread " + currentThread().getId() + "] Chunk " + chunk + " unlocked.");
				}
			}
		}
	}
}
