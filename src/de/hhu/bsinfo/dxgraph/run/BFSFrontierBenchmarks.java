/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
import de.hhu.bsinfo.dxutils.eval.EvaluationTables;
import de.hhu.bsinfo.dxutils.eval.Stopwatch;

/**
 * Benchmark and compare execution time of various frontier lists used for BFS in dxgraph.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class BFSFrontierBenchmarks {
    private EvaluationTables m_tables;

    /**
     * Constructor
     */
    private BFSFrontierBenchmarks() {

    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *         Main arguments.
     */
    public static void main(final String[] p_args) {
        BFSFrontierBenchmarks benchmark = new BFSFrontierBenchmarks();
        benchmark.run(p_args);
    }

    public void run(final String[] p_args) {
        if (p_args.length < 3) {
            System.out.println("Usage: [itemCount] [threads] [randDistributionFillRate]");
            return;
        }

        int itemCount = Integer.parseInt(p_args[0]);
        int threads = Integer.parseInt(p_args[1]);
        float randDistributionFillRate = Float.parseFloat(p_args[2]);

        prepareTable();

        main(itemCount, threads, randDistributionFillRate);
        // mainEval(threads);

        System.out.println(m_tables.toCsv(true, "\t"));
    }

    /**
     * Execute a full evaluation with a range of parameters.
     *
     * @param p_threads
     *         Number of threads for multi threaded part.
     */
    @SuppressWarnings("unused")
    private void mainEval(final int p_threads) {
        for (int i = 100; i <= 1000000; i *= 10) {
            // don't use a for loop, because floating point arithmetic
            // causes rounding issues
            main(i, p_threads, 0.1f);
            main(i, p_threads, 0.2f);
            main(i, p_threads, 0.3f);
            main(i, p_threads, 0.4f);
            main(i, p_threads, 0.5f);
            main(i, p_threads, 0.6f);
            main(i, p_threads, 0.7f);
            main(i, p_threads, 0.8f);
            main(i, p_threads, 0.9f);
            main(i, p_threads, 1.0f);
        }
    }

    /**
     * Execute a single evaluation pass.
     *
     * @param p_itemCount
     *         Number of max items for a frontier.
     * @param p_threads
     *         Number of threads to use for multi threaded section.
     * @param p_randDistFillRate
     *         Distribution/Fill rate of of items in a frontier. 0.8 means that a frontier
     *         will be filled with 80% of p_itemCount elements.
     */
    private void main(final int p_itemCount, final int p_threads, final float p_randDistFillRate) {
        System.out.println("=======================================================================");
        System.out.println("Creating test data, totalItemCount " + p_itemCount + ", fill rate " + p_randDistFillRate);
        long[] testData = createTestData(p_itemCount, p_randDistFillRate);
        long testVector = createTestVector(testData);

        if (!doSingleThreaded(p_itemCount, testData, testVector, "SingleThreaded/" + p_itemCount, Float.toString(p_randDistFillRate))) {
            return;
        }

        // if (!doMultiThreaded(p_itemCount, p_threads, testData, testVector, "MultiThreaded/" + p_itemCount,
        // Float.toString(p_randDistFillRate))) {
        // return;
        // }

        System.out.println("Execution done.");
        System.out.println("--------------------------");
    }

    /**
     * Prepare the table for recording data.
     */
    private void prepareTable() {
        m_tables = new EvaluationTables(12, 8, 10);
        m_tables.setTableName(0, "SingleThreaded/100/pushBack");
        m_tables.setTableName(1, "SingleThreaded/1000/pushBack");
        m_tables.setTableName(2, "SingleThreaded/10000/pushBack");
        m_tables.setTableName(3, "SingleThreaded/100000/pushBack");
        m_tables.setTableName(4, "SingleThreaded/1000000/pushBack");
        m_tables.setTableName(5, "SingleThreaded/100/popFront");
        m_tables.setTableName(6, "SingleThreaded/1000/popFront");
        m_tables.setTableName(7, "SingleThreaded/10000/popFront");
        m_tables.setTableName(8, "SingleThreaded/100000/popFront");
        m_tables.setTableName(9, "SingleThreaded/1000000/popFront");

        m_tables.setIntersectTopCornerNames("DataStructure");

        m_tables.setColumnNames(0, "0.1");
        m_tables.setColumnNames(1, "0.2");
        m_tables.setColumnNames(2, "0.3");
        m_tables.setColumnNames(3, "0.4");
        m_tables.setColumnNames(4, "0.5");
        m_tables.setColumnNames(5, "0.6");
        m_tables.setColumnNames(6, "0.7");
        m_tables.setColumnNames(7, "0.8");
        m_tables.setColumnNames(8, "0.9");
        m_tables.setColumnNames(9, "1.0");

        m_tables.setRowNames(0, "BulkFifoNaive");
        m_tables.setRowNames(1, "BulkFifo");
        m_tables.setRowNames(2, "TreeSetFifo");
        m_tables.setRowNames(3, "BitVector");
        m_tables.setRowNames(4, "BitVectorWithStartPos");
        m_tables.setRowNames(5, "BitVectorMultiLevel");
        m_tables.setRowNames(6, "HalfConcurrentBitVector");
        m_tables.setRowNames(7, "ConcurrentBitVector");
    }

    /**
     * Prepare the data structures to be tested on the single test pass.
     *
     * @param p_itemCount
     *         Number of max items for a frontier.
     * @return List of frontier lists to be executed on the single thread pass.
     */
    private ArrayList<FrontierList> prepareTestsSingleThreaded(final long p_itemCount) {
        ArrayList<FrontierList> list = new ArrayList<>();

        list.add(new BulkFifoNaive());
        list.add(new BulkFifo());
        list.add(new TreeSetFifo(p_itemCount));
        list.add(new BitVector(p_itemCount));
        list.add(new BitVectorWithStartPos(p_itemCount));
        list.add(new BitVectorMultiLevel(p_itemCount));
        list.add(new HalfConcurrentBitVector(p_itemCount));
        list.add(new ConcurrentBitVector(p_itemCount));

        return list;
    }

    /**
     * Prepare the data structures to be tested on the multi thread test pass.
     *
     * @param p_itemCount
     *         Number of max items for a frontier.
     * @return List of frontier lists to be executed on the multi thread pass.
     */
    private ArrayList<FrontierList> prepareTestsMultiThreaded(final long p_itemCount) {
        ArrayList<FrontierList> list = new ArrayList<>();

        list.add(new ConcurrentBitVector(p_itemCount));

        return list;
    }

    /**
     * Execute the test of one data structure single threaded.
     *
     * @param p_frontierList
     *         Frontier list to test/benchmark.
     * @param p_testData
     *         Test data to be used.
     * @param p_table
     *         Name of the table to put the recorded data into.
     * @param p_column
     *         Name of the column in the table to put recorded data into.
     * @return Test vector to verify if test data was successfully written and read back.
     */
    private long executeTestSingleThreaded(final FrontierList p_frontierList, final long[] p_testData, final String p_table, final String p_column) {
        Stopwatch stopWatch = new Stopwatch();

        System.out.println("Pushing back data...");
        {
            stopWatch.start();
            for (long testData : p_testData) {
                p_frontierList.pushBack(testData);
            }
            stopWatch.stop();
            m_tables.set(p_table + "/pushBack", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
            stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack");
        }
        System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) p_testData.length);

        System.out.println("Popping data front...");
        long vals = 0;
        {
            stopWatch.start();
            for (long testData : p_testData) {
                vals += p_frontierList.popFront();
            }
            stopWatch.stop();
            m_tables.set(p_table + "/popFront", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
            stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront");
        }

        return vals;
    }

    /**
     * Execute the test of one data structure multi threaded.
     *
     * @param p_frontierList
     *         Frontier list to test/benchmark.
     * @param p_threadCount
     *         Number of threads to start
     * @param p_testData
     *         Test data to be used.
     * @param p_table
     *         Name of the table to put the recorded data into.
     * @param p_column
     *         Name of the column in the table to put recorded data into.
     * @return Test vector to verify if test data was successfully written and read back.
     */
    private long executeTestMultiThreaded(final FrontierList p_frontierList, final int p_threadCount, final long[] p_testData, final String p_table,
            final String p_column) {
        Stopwatch stopWatch = new Stopwatch();

        System.out.println("Pushing back data...");
        {
            PushWorkerThread[] threads = new PushWorkerThread[p_threadCount];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new PushWorkerThread();
                threads[i].m_frontier = p_frontierList;
                threads[i].m_testData = new long[p_testData.length / threads.length];
                System.arraycopy(p_testData, i * (p_testData.length / threads.length), threads[i].m_testData, 0, threads[i].m_testData.length);
                threads[i].start();
            }

            // wait a moment to ensure all threads are started
            try {
                Thread.sleep(2000);
            } catch (final InterruptedException e1) {
                e1.printStackTrace();
            }

            stopWatch.start();
            for (PushWorkerThread thread : threads) {
                thread.m_wait = false;
            }

            for (PushWorkerThread thread : threads) {
                try {
                    thread.join();
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
            stopWatch.stop();
            m_tables.set(p_table + "/pushBack", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
            stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack");
        }
        System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) p_testData.length);

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
            for (PopWorkerThread thread : threads) {
                thread.m_wait = false;
            }

            for (PopWorkerThread thread : threads) {
                try {
                    thread.join();
                    val += thread.m_val;
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
            stopWatch.stop();
            m_tables.set(p_table + "/popFront", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
            stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront");
        }

        return val;
    }

    /**
     * Do a full single thread pass with given parameters on all prepared frontiers.
     *
     * @param p_itemCount
     *         Max number of items for a single frontier.
     * @param p_testData
     *         Test data to be used.
     * @param p_testVector
     *         Test vector of the test data for verification.
     * @param p_table
     *         Name of the table to put the recorded data into.
     * @param p_column
     *         Name of the column in the table to put recorded data into.
     * @return True if execution was successful and validation ok, false otherwise.
     */
    private boolean doSingleThreaded(final int p_itemCount, final long[] p_testData, final long p_testVector, final String p_table, final String p_column) {
        System.out.println("---------------------------------------------------");
        System.out.println("Single threaded tests");
        ArrayList<FrontierList> frontiersToTest = prepareTestsSingleThreaded(p_itemCount);

        for (FrontierList frontier : frontiersToTest) {
            System.out.println("-------------");
            System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
            long resVector = executeTestSingleThreaded(frontier, p_testData, p_table, p_column);

            System.out.println("Data validation...");
            if (resVector != p_testVector) {
                System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
                return false;
            }
        }

        return true;
    }

    /**
     * Do a full multi thread pass with given parameters on all prepared frontiers.
     *
     * @param p_itemCount
     *         Max number of items for a single frontier.
     * @param p_threadCount
     *         Number of threads to start
     * @param p_testData
     *         Test data to be used.
     * @param p_testVector
     *         Test vector of the test data for verification.
     * @param p_table
     *         Name of the table to put the recorded data into.
     * @param p_column
     *         Name of the column in the table to put recorded data into.
     * @return True if execution was successful and validation ok, false otherwise.
     */
    @SuppressWarnings("unused")
    private boolean doMultiThreaded(final int p_itemCount, final int p_threadCount, final long[] p_testData, final long p_testVector, final String p_table,
            final String p_column) {
        System.out.println("---------------------------------------------------");
        System.out.println("Multi threaded tests, threads: " + p_threadCount);
        ArrayList<FrontierList> frontiersToTest = prepareTestsMultiThreaded(p_itemCount);

        for (FrontierList frontier : frontiersToTest) {
            System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
            long resVector = executeTestMultiThreaded(frontier, p_threadCount, p_testData, p_table, p_column);

            System.out.println("Data validation...");
            if (resVector != p_testVector) {
                System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
                // return false;
            }
        }

        return true;
    }

    /**
     * Create the test data with given parameters.
     *
     * @param p_totalItemCount
     *         Max number of items for the test data.
     * @param p_randDistFillRate
     *         Distribution/Fill rate for the test data.
     * @return Array with shuffled test data.
     */
    private long[] createTestData(final int p_totalItemCount, final float p_randDistFillRate) {
        long[] testData;
        if (p_randDistFillRate < 1.0f && p_randDistFillRate > 0.0f) {
            System.out.println("Creating random distribution test data...");

            Random rand = new Random();
            TreeSet<Long> set = new TreeSet<>();
            testData = new long[(int) (p_totalItemCount * p_randDistFillRate)];
            int setCount = 0;

            // use a set to ensure generating non duplicates until we hit the specified fill rate
            while (((float) setCount / p_totalItemCount) < p_randDistFillRate) {
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

    /**
     * Shuffle the contents of an array.
     *
     * @param p_array
     *         Array with contents to shuffle.
     */
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

    /**
     * Create the test vector for verification.
     *
     * @param p_testData
     *         Test data to create the test vector of.
     * @return Test vector.
     */
    private long createTestVector(final long[] p_testData) {
        long testVec = 0;
        for (long testData : p_testData) {
            testVec += testData;
        }
        return testVec;
    }

    /**
     * Thread for multi thread pass to push back the data concurrently.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
     */
    private static class PushWorkerThread extends Thread {
        FrontierList m_frontier;
        long[] m_testData;
        volatile boolean m_wait = true;

        @Override
        public void run() {
            while (m_wait) {
                // busy loop to avoid latency on start
                Thread.yield();
            }

            for (long testData : m_testData) {
                m_frontier.pushBack(testData);
            }
        }
    }

    /**
     * Thread for multi thread pass to pop the data from the front concurrently.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
     */
    private static class PopWorkerThread extends Thread {
        FrontierList m_frontier;
        volatile long m_val;
        volatile boolean m_wait = true;

        @Override
        public void run() {
            while (m_wait) {
                // busy loop to avoid latency on start
                Thread.yield();
            }

            long val = 0;
            while (true) {
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
