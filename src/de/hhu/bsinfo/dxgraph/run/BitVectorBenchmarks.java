package de.hhu.bsinfo.dxgraph.run;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorMultiLevel;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorWithStartPos;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifo;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifoNaive;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.HalfConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.TreeSetFifo;
import de.hhu.bsinfo.utils.Stopwatch;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

public class BitVectorBenchmarks extends Main {

	private static final Argument ARG_ITEM_COUNT = new Argument("itemCount", "100", true, "Number of items to add and get from the list. "
			+ "Make sure this is a multiple of the thread count, otherwise the multi threaded part will fail on validation");
	private static final Argument ARG_THREADS = new Argument("threads", "2", true, "Number of threads for the multi threaded section");
	private static final Argument ARG_ITEM_COUNT_RAND_FILL_RATE = new Argument("itemCountRandDistFillRate", "1.0", true, "Enables random "
			+ "distribution if value is < 1.0 and > 0.0 when pushing items and defines the fill rate for the vector with 1.0 being 100%, i.e. full vector"); 
	
	private static final boolean MS_PRINT_READABLE_TIME = false; 
	
	public static void main(final String[] args) {
		Main main = new BitVectorBenchmarks();
		main.run(args);
	}
	
	protected BitVectorBenchmarks() {
		super("Test the various BitVector implementations and measure execution time.");
		
	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_ITEM_COUNT);
		p_arguments.setArgument(ARG_THREADS);
		p_arguments.setArgument(ARG_ITEM_COUNT_RAND_FILL_RATE);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		int itemCount = p_arguments.getArgument(ARG_ITEM_COUNT).getValue(Integer.class);
		int threads = p_arguments.getArgument(ARG_THREADS).getValue(Integer.class);
		float randDistributionFillRate = p_arguments.getArgument(ARG_ITEM_COUNT_RAND_FILL_RATE).getValue(Float.class);
		
		//main(itemCount, threads, randDistributionFillRate);
		
		for (int j = 100; j <= 1000000; j *= 10)
		{
			for (int i = 1; i <= 10; i++)
			{
				main(j, threads, i / 10.f);
			}
		}

		
		return 0;
	}
	
	private void main(final int p_itemCount, final int p_threads, final float p_randDistFillRate)
	{
		System.out.println("=======================================================================");
		System.out.println("Creating test data, totalItemCount " + p_itemCount + ", fill rate " + p_randDistFillRate);
		long[] testData = createTestData(p_itemCount, p_randDistFillRate);
		long testVector = createTestVector(testData);
		
		if (!doSingleThreaded(p_itemCount, testData, testVector)) {
			return;
		}
		
//		if (!doMultiThreaded(p_itemCount, p_threads, testData, testVector)) {
//			return;
//		}
		
		System.out.println("Execution done.");
	}
	
	private ArrayList<FrontierList> prepareTestsSingleThreaded(final long p_itemCount)
	{
		ArrayList<FrontierList> list = new ArrayList<FrontierList>();
		
		list.add(new BulkFifoNaive());
		list.add(new BulkFifo());
		list.add(new TreeSetFifo());
		list.add(new BitVector(p_itemCount));
		list.add(new BitVectorWithStartPos(p_itemCount));
		list.add(new BitVectorMultiLevel(p_itemCount));
		list.add(new HalfConcurrentBitVector(p_itemCount));
		list.add(new ConcurrentBitVector(p_itemCount));
		
		return list;
	}
	
	private ArrayList<FrontierList> prepareTestsMultiThreaded(final long p_itemCount)
	{
		ArrayList<FrontierList> list = new ArrayList<FrontierList>();
		
//		list.add(new BulkFifoNaive());
//		list.add(new BulkFifo());
//		list.add(new TreeSetFifo());
//		list.add(new BitVector(p_itemCount));
//		list.add(new BitVectorOptimized(p_itemCount));
//		list.add(new HalfConcurrentBitVector(p_itemCount));
		list.add(new ConcurrentBitVector(p_itemCount));
		
		return list;
	}

	private long executeTestSingleThreaded(final FrontierList p_frontierList, final long[] testData) {
		Stopwatch stopWatch = new Stopwatch();
				
		System.out.println("Pushing back data...");
		{
			stopWatch.start();
			for (int i = 0; i < testData.length; i++) {
				p_frontierList.pushBack(testData[i]);
			}
			stopWatch.stop();
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack", MS_PRINT_READABLE_TIME);
		}
		System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) testData.length);
		
		System.out.println("Popping data front...");
		long vals = 0;
		{
			stopWatch.start();
			for (long i = 0; i < testData.length; i++) {
				vals += p_frontierList.popFront();
			}
			stopWatch.stop();
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront", MS_PRINT_READABLE_TIME);
		}

		return vals;
	}
	
	private long executeTestMultiThreaded(final FrontierList p_frontierList, final int p_threadCount, final long[] testData) {
		Stopwatch stopWatch = new Stopwatch();
		
		System.out.println("Pushing back data...");
		{
			PushWorkerThread[] threads = new PushWorkerThread[p_threadCount];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new PushWorkerThread();
				threads[i].m_frontier = p_frontierList;
				threads[i].m_testData = new long[testData.length / threads.length];
				System.arraycopy(testData, i * (testData.length / threads.length), threads[i].m_testData, 0, threads[i].m_testData.length);				
				threads[i].start();
			}
			
			// wait a moment to ensure all threads are started
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			stopWatch.start();
			for (int i = 0; i < threads.length; i++) {
				threads[i].m_wait = false;
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			stopWatch.stop();
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack", MS_PRINT_READABLE_TIME);
		}
		System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) testData.length);
		
		System.out.println("Popping data front...");
		long val = 0;
		{
			PopWorkerThread[] threads = new PopWorkerThread[p_threadCount];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new PopWorkerThread();
				threads[i].m_frontier = p_frontierList;
				threads[i].start();
			}
			
			stopWatch.start();
			for (int i = 0; i < threads.length; i++) {
				threads[i].m_wait = false;
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
					val += threads[i].m_val;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			stopWatch.stop();
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront", MS_PRINT_READABLE_TIME);
		}
		
		return val;
	}
	
	private boolean doSingleThreaded(final int p_itemCount, final long[] p_testData, final long p_testVector) {
		System.out.println("---------------------------------------------------");
		System.out.println("Single threaded tests");
		ArrayList<FrontierList> frontiersToTest = prepareTestsSingleThreaded(p_itemCount);
		
		for (FrontierList frontier : frontiersToTest) {
			System.out.println("-------------");
			System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
			long resVector = executeTestSingleThreaded(frontier, p_testData);
			
			System.out.println("Data validation...");
			if (resVector != p_testVector) {
				System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
				//return false;
			}
		}
		
		return true;
	}
	
	private boolean doMultiThreaded(final int p_itemCount, final int p_threadCount, final long[] p_testData, final long p_testVector) {
		System.out.println("---------------------------------------------------");
		System.out.println("Multi threaded tests, threads: " + p_threadCount);
		ArrayList<FrontierList> frontiersToTest = prepareTestsMultiThreaded(p_itemCount);
		
		for (FrontierList frontier : frontiersToTest) {
			System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
			long resVector = executeTestMultiThreaded(frontier, p_threadCount, p_testData);
			
			System.out.println("Data validation...");
			if (resVector != p_testVector) {
				System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
				//return false;
			}
		}
		
		return true;
	}
	
	private long[] createTestData(final int p_totalItemCount, final float p_randDistFillRate)
	{
		long[] testData;
		if (p_randDistFillRate < 1.0f && p_randDistFillRate > 0.0f) {
			System.out.println("Creating random distribution test data...");
			
			Random rand = new Random();
			TreeSet<Long> set = new TreeSet<Long>();
			testData = new long[(int) (p_totalItemCount * p_randDistFillRate)];
			int setCount = 0;

			// use a set to ensure generating non duplicates until we hit the specified fill rate
			while (((float) setCount / p_totalItemCount) < p_randDistFillRate)
			{
				if (set.add((long) (rand.nextFloat() * p_totalItemCount))) {
					setCount++;
				}
			}

			for (int i = 0; i < testData.length; i++) {
				testData[i] = set.pollFirst();
			}
			shuffleArray(testData);
		} else {
			System.out.println("Creating continous test data...");
			
			testData = new long[p_totalItemCount];
			for (int i = 0; i < testData.length; i++) {
				testData[i] = i;
			}
		}
		return testData;
	}
	
	private static void shuffleArray(final long[] p_array) {
		Random rnd = new Random();
		for (int i = p_array.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			long a = p_array[index];
			p_array[index] = p_array[i];
			p_array[i] = a;
		}
	}
	
	private long createTestVector(final long[] p_testData) {
		long testVec = 0;
		for (int i = 0; i < p_testData.length; i++) {
			testVec += p_testData[i];
		}
		return testVec;
	}
	
	private static class PushWorkerThread extends Thread
	{
		public FrontierList m_frontier = null;
		public long[] m_testData = null;
		public volatile boolean m_wait = true;
		
		@Override
		public void run()
		{
			while (m_wait)
			{
				// busy loop to avoid latency on start
			}
			
			for (int i = 0; i < m_testData.length; i++) {
				m_frontier.pushBack(m_testData[i]);
			}
		}
	}
	
	private static class PopWorkerThread extends Thread
	{
		public FrontierList m_frontier = null;
		public volatile long m_val = 0;
		public volatile boolean m_wait = true;
		
		@Override
		public void run()
		{
			while (m_wait)
			{
				// busy loop to avoid latency on start
			}
			
			long val = 0;
			while (true)
			{
				long tmp = m_frontier.popFront();
				if (tmp == -1) {
					break;
				}
				val += tmp;
			}
			
			m_val = val;
		}
	}
}
