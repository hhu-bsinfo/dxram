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

package de.hhu.bsinfo.soh;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Integrity checker and tests for the SmallObjectHeap
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class SmallObjectHeapIntegrityTest {
    private static final Lock LOCK = new ReentrantLock(false);

    private SmallObjectHeap m_memory;
    private int m_testId = -1;
    private int m_numThreads = -1;
    private int m_numOperations = -1;
    private float m_mallocFreeRatio;
    private int m_blockSizeMin = -1;
    private int m_blockSizeMax = -1;
    private int m_multiMallocCount;
    private boolean m_debugPrint;

    /**
     * Constructor
     *
     * @param p_testId
     *     Identifier for the test
     * @param p_memorySize
     *     Total raw memory size in bytes.
     * @param p_maxBlockSize
     *     Maximum size of a single memory block
     * @param p_numThreads
     *     Number of threads to run this
     * @param p_numOperations
     *     Number of operations to execute.
     * @param p_mallocFreeRatio
     *     Malloc/free ratio.
     * @param p_blockSizeMin
     *     Minimum memory block size to alloc.
     * @param p_blockSizeMax
     *     Maximum memory block size to alloc.
     * @param p_debugPrint
     *     Enable debug prints
     */
    private SmallObjectHeapIntegrityTest(final int p_testId, final long p_memorySize, final int p_maxBlockSize, final int p_numThreads,
        final int p_numOperations, final float p_mallocFreeRatio, final int p_blockSizeMin, final int p_blockSizeMax, final int p_multiMallocCount,
        final boolean p_debugPrint) {
        assert p_memorySize > 0;
        assert p_numThreads > 0;
        assert p_mallocFreeRatio >= 0.5;
        assert p_blockSizeMin > 0;
        assert p_blockSizeMax > 0;
        assert p_blockSizeMax >= p_blockSizeMin;

        m_memory = new SmallObjectHeap(new StorageUnsafeMemory(), p_memorySize, p_maxBlockSize);

        m_testId = p_testId;
        m_numThreads = p_numThreads;
        m_numOperations = p_numOperations;
        m_mallocFreeRatio = p_mallocFreeRatio;
        m_blockSizeMin = p_blockSizeMin;
        m_blockSizeMax = p_blockSizeMax;
        m_multiMallocCount = p_multiMallocCount;
        m_debugPrint = p_debugPrint;
    }

    /**
     * Main section
     */
    public void run() {
        ExecutorService executor = Executors.newFixedThreadPool(m_numThreads);

        System.out.println(m_memory);

        System.out.println("Starting " + m_numThreads + " threads.");
        ArrayList<Future<?>> submittedTasks = new ArrayList<>();
        for (int i = 0; i < m_numThreads; i++) {
            MemoryThread memThread =
                new MemoryThread(m_memory, m_numOperations, m_mallocFreeRatio, m_blockSizeMin, m_blockSizeMax, m_multiMallocCount, m_debugPrint);
            submittedTasks.add(executor.submit(memThread));
        }

        System.out.println("Waiting for workers to finish...");

        try {
            for (Future<?> future : submittedTasks) {
                try {
                    future.get();
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (final ExecutionException mem) {
            executor.shutdown();

            System.out.println("!!! ERROR: " + mem.getMessage());
            mem.printStackTrace();
            String fileName = "SohInTest-" + m_testId + ".dump";

            System.out.println("Dumping memory to " + fileName);
            m_memory.dump(fileName);

            System.exit(-1);
        }

        System.out.println("All workers finished.");

        executor.shutdown();

        m_memory.destroy();
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Command line arguments
     */
    public static void main(final String[] p_args) {
        if (p_args.length >= 1 && p_args.length < 8) {
            System.out.println(
                "Usage: RawMemoryTest <memorySize> <maxBlockSize> <numThreads> <numOperations> <mallocFreeRatio> <blockSizeMin> <blockSizeMax> " +
                    "<multiMallocCount> <debugPrint>");
            return;
        }

        if (p_args.length == 0) {
            runTests();
        } else {
            long memorySize = Long.parseLong(p_args[0]);
            int maxBlockSize = Integer.parseInt(p_args[1]);
            int numThreads = Integer.parseInt(p_args[2]);
            int numOperations = Integer.parseInt(p_args[3]);
            float mallocFreeRatio = Float.parseFloat(p_args[4]);
            int blockSizeMin = Integer.parseInt(p_args[5]);
            int blockSizeMax = Integer.parseInt(p_args[6]);
            int multiMallocCount = Integer.parseInt(p_args[7]);
            boolean debugPrint = Boolean.parseBoolean(p_args[8]);

            runTest(0, memorySize, maxBlockSize, numThreads, numOperations, mallocFreeRatio, blockSizeMin, blockSizeMax, multiMallocCount, debugPrint);
        }
    }

    /**
     * List of hardcoded tests to ensure everything's working
     */
    private static void runTests() {
        final int maxBlockSize = 1024 * 1024 * 8;
        final int maxBlockSizeHuge = 1024 * 1024 * 16;

        // create heap, only
        runTest(0, 1024, maxBlockSize, 1, 0, 0.5f, 16, 16, 0, false);

        // single malloc
        runTest(1, 1024, maxBlockSize, 1, 1, 1.0f, 16, 16, 0, false);
        // a few more mallocs
        runTest(2, 1024, maxBlockSize, 1, 3, 1.0f, 16, 16, 0, false);
        // single malloc + free
        runTest(3, 1024, maxBlockSize, 1, 2, 0.5f, 16, 16, 0, false);
        // multiple malloc + free
        runTest(4, 1024, maxBlockSize, 1, 6, 0.5f, 16, 16, 0, false);

        // greater tests, malloc only
        runTest(5, 1024 * 1024, maxBlockSize, 1, 10, 1.0f, 16, 1024, 0, false);
        runTest(6, 1024 * 1024 * 1024, maxBlockSize, 1, 100, 1.0f, 16, 1024 * 1024, 0, false);
        runTest(7, 1024 * 1024 * 1024, maxBlockSize, 1, 1000, 1.0f, 16, 1024 * 1024, 0, false);
        runTest(8, 1024 * 1024 * 1024, maxBlockSize, 1, 5000, 1.0f, 16, 1024 * 100, 0, false);
        runTest(9, 1024 * 1024 * 1024, maxBlockSize, 1, 100000, 1.0f, 16, 1024 * 10, 0, false);
        runTest(10, 1024 * 1024 * 1024, maxBlockSize, 1, 1000000, 1.0f, 16, 1024, 0, false);

        // some greater tests with malloc and free
        runTest(11, 1024 * 1024, maxBlockSize, 1, 10, 0.5f, 16, 1024, 0, false);
        runTest(12, 1024 * 1024 * 1024, maxBlockSize, 1, 100, 0.5f, 16, 1024 * 1024, 0, false);
        runTest(13, 1024 * 1024 * 1024, maxBlockSize, 1, 1000, 0.5f, 16, 1024 * 1024, 0, false);
        runTest(14, 1024 * 1024 * 1024, maxBlockSize, 1, 10000, 0.5f, 16, 1024 * 1024, 0, false);
        runTest(15, 1024 * 1024 * 1024, maxBlockSize, 1, 100000, 0.5f, 16, 1024 * 1024, 0, false);
        runTest(16, 1024 * 1024 * 1024, maxBlockSize, 1, 1000000, 0.5f, 16, 1024 * 1024, 0, false);

        // multi malloc
        runTest(17, 1024, maxBlockSize, 1, 2, 1.0f, 16, 16, 2, false);
        runTest(18, 1024, maxBlockSize, 1, 4, 0.5f, 16, 16, 2, false);
        runTest(19, 1024 * 1024 * 1024, maxBlockSize, 1, 100, 1.0f, 16, 1024 * 1024, 10, false);

        runTest(20, 1024 * 1024 * 1024, maxBlockSize, 1, 100, 0.5f, 16, 1024 * 1024, 10, false);
        runTest(21, 1024 * 1024 * 1024 * 2L, maxBlockSize, 1, 1000, 0.5f, 16, 1024 * 1024, 10, false);
        runTest(22, 1024 * 1024 * 1024 * 4L, maxBlockSize, 1, 10000, 0.5f, 16, 1024 * 1024, 10, false);

        runTest(23, 1024 * 1024 * 1024 * 16L, maxBlockSize, 1, 10000, 0.5f, 16, 1024 * 1024, 10, false);
        runTest(24, 1024 * 1024 * 1024 * 16L, maxBlockSize, 1, 10000, 0.5f, 16, 1024 * 1024, 100, false);
        runTest(25, 1024 * 1024 * 1024 * 16L, maxBlockSize, 1, 10000, 0.5f, 16, 1024 * 1024, 1000, false);

        // huge blocks
        runTest(26, 1024 * 1024 * 10, maxBlockSizeHuge, 1, 1, 1.0f, 1024 * 1024 * 9, 1024 * 1024 * 9, 0, false);
        runTest(27, 1024 * 1024 * 128, maxBlockSizeHuge, 1, 6, 0.5f, 1024 * 1024 * 9, 1024 * 1024 * 16, 0, false);
        runTest(28, 1024 * 1024 * 1024, maxBlockSizeHuge, 1, 100, 0.5f, 1024 * 1024 * 9, 1024 * 1024 * 16, 0, false);

        runTest(29, 1024 * 1024 * 1024, maxBlockSizeHuge, 1, 100, 0.5f, 1024 * 1024 * 9, 1024 * 1024 * 16, 10, false);
    }

    /**
     * Run the test
     *
     * @param p_testId
     *     Id of the test run
     * @param p_memorySize
     *     Size of the memory
     * @param p_maxBlockSize
     *     Maximum size of a single block in memory
     * @param p_numThreads
     *     Number of threads to run
     * @param p_numOperations
     *     Number of operations to execute
     * @param p_mallocFreeRatio
     *     Ratio of malloc and frees
     * @param p_blockSizeMin
     *     Minimal block size on allocation
     * @param p_blockSizeMax
     *     Maximum block size on allocation
     * @param p_debugPrint
     *     Print debug information
     */
    private static void runTest(final int p_testId, final long p_memorySize, final int p_maxBlockSize, final int p_numThreads, final int p_numOperations,
        final float p_mallocFreeRatio, final int p_blockSizeMin, final int p_blockSizeMax, final int p_multiMallocCount, final boolean p_debugPrint) {

        System.out.println("===============================================================");
        System.out.println("Initializing RawMemory test (" + p_testId + ")...");
        SmallObjectHeapIntegrityTest test =
            new SmallObjectHeapIntegrityTest(p_testId, p_memorySize, p_maxBlockSize, p_numThreads, p_numOperations, p_mallocFreeRatio, p_blockSizeMin,
                p_blockSizeMax, p_multiMallocCount, p_debugPrint);
        System.out.println("Running test (" + p_testId + ")...");
        long timeStart = System.nanoTime();
        test.run();
        long time = System.nanoTime() - timeStart;
        System.out.println("Test (" + p_testId + ") done: " + time / 1000.0 / 1000.0 + " ms");
    }

    /**
     * Thread execution memory allocations.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
     */
    private static class MemoryThread implements Runnable {
        private SmallObjectHeap m_memory;

        private float m_mallocFreeRatio;
        private int m_numMallocOperations = -1;
        private int m_numFreeOperations = -1;
        private int m_blockSizeMin = -1;
        private int m_blockSizeMax = -1;
        private int m_multiMallocCount = -1;
        private boolean m_debugPrint;

        private ArrayList<Long> m_blocksAlloced = new ArrayList<>();

        /**
         * Constructor
         *
         * @param p_rawMemory
         *     Raw memory instance to use.
         * @param p_numOperations
         *     Number of operations to execute.
         * @param p_mallocFreeRatio
         *     Malloc/free ratio.
         * @param p_blockSizeMin
         *     Minimum memory block size to alloc.
         * @param p_blockSizeMax
         *     Maximum memory block size to alloc.
         * @param p_debugPrint
         *     Enable debug prints
         */
        MemoryThread(final SmallObjectHeap p_rawMemory, final int p_numOperations, final float p_mallocFreeRatio, final int p_blockSizeMin,
            final int p_blockSizeMax, final int p_multiMallocCount, final boolean p_debugPrint) {
            assert p_blockSizeMin > 0;
            assert p_blockSizeMax > 0;
            assert p_mallocFreeRatio > 0;

            m_memory = p_rawMemory;
            m_blockSizeMin = p_blockSizeMin;
            m_blockSizeMax = p_blockSizeMax;
            m_mallocFreeRatio = p_mallocFreeRatio;
            m_multiMallocCount = p_multiMallocCount;
            m_debugPrint = p_debugPrint;

            if (m_blockSizeMax < m_blockSizeMin) {
                m_blockSizeMax = m_blockSizeMin;
            }

            if (p_mallocFreeRatio < 0.5) {
                m_numMallocOperations = p_numOperations / 2;
                m_numFreeOperations = m_numMallocOperations;
            } else {
                m_numMallocOperations = (int) (p_numOperations * p_mallocFreeRatio);
                m_numFreeOperations = p_numOperations - m_numMallocOperations;
            }
        }

        @Override
        public void run() {
            System.out.println("(" + Thread.currentThread().getId() + ") " + this);

            while (m_numMallocOperations + m_numFreeOperations > 0) {
                // random pick operations, depending on ratio
                if ((Math.random() <= m_mallocFreeRatio || m_numFreeOperations <= 0) && m_numMallocOperations > 0) {

                    if (m_multiMallocCount > 0) {

                        int mallocCount = m_multiMallocCount;
                        if (m_numMallocOperations < m_multiMallocCount) {
                            mallocCount = m_numMallocOperations;
                        }

                        m_numMallocOperations -= mallocCount;

                        int[] sizes = new int[mallocCount];

                        for (int i = 0; i < sizes.length; i++) {
                            while (sizes[i] <= 0) {
                                sizes[i] = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin)) + m_blockSizeMin;
                            }
                        }

                        LOCK.lock();
                        long[] ptrs = m_memory.multiMallocSizes(sizes);
                        LOCK.unlock();

                        if (ptrs == null) {
                            System.out.println("Multi malloc failed, check for out of memory, otherwise error");
                            System.out.println(m_memory.getStatus());
                            System.exit(-1);
                        }

                        for (int i = 0; i < ptrs.length; i++) {
                            m_blocksAlloced.add(ptrs[i]);
                            m_memory.set(ptrs[i], sizes[i], (byte) 0xBB);
                        }
                    } else {
                        // execute alloc
                        int size = 0;
                        while (size <= 0) {
                            size = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin)) + m_blockSizeMin;
                        }

                        LOCK.lock();
                        long ptr = m_memory.malloc(size);
                        LOCK.unlock();

                        if (ptr == SmallObjectHeap.INVALID_ADDRESS) {
                            System.out.println("!!! Allocation of size " + size + " failed, check for out of memory, otherwise error");
                            System.out.println(m_memory.getStatus());
                            System.exit(-1);
                        }

                        if (m_debugPrint) {
                            System.out.println(">>> Allocated " + size + ":\n" + m_memory);
                        }

                        m_blocksAlloced.add(ptr);
                        m_memory.set(ptr, size, (byte) 0xBB);

                        m_numMallocOperations--;

                        // test writing/reading across two blocks
                        if (size > m_memory.getStatus().getMaxBlockSize()) {

                            {
                                if (m_memory.getSizeBlock(ptr) <= m_memory.getStatus().getMaxBlockSize()) {
                                    System.out.println("!!! Chained blocks getting size failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                short v = 0x1122;
                                m_memory.writeShort(ptr, m_memory.getStatus().getMaxBlockSize() - 1, v);
                                short v2 = m_memory.readShort(ptr, m_memory.getStatus().getMaxBlockSize() - 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks short writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                int v = 0xAABBCCDD;
                                m_memory.writeInt(ptr, m_memory.getStatus().getMaxBlockSize() - 1, v);
                                int v2 = m_memory.readInt(ptr, m_memory.getStatus().getMaxBlockSize() - 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks int (1) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                int v = 0xAABBCCDD;
                                m_memory.writeInt(ptr, m_memory.getStatus().getMaxBlockSize() - 2, v);
                                int v2 = m_memory.readInt(ptr, m_memory.getStatus().getMaxBlockSize() - 2);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks int (2) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                int v = 0xAABBCCDD;
                                m_memory.writeInt(ptr, m_memory.getStatus().getMaxBlockSize() - 3, v);
                                int v2 = m_memory.readInt(ptr, m_memory.getStatus().getMaxBlockSize() - 3);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks int (3) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 1, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (1) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 2, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 2);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (2) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 3, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 3);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (3) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 4, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 4);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (4) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 5, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 5);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (5) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 6, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 6);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (6) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 7, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 7);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (7) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() - 8, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() - 8);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long (8) writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeBytes(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test, 0, test.length);
                                byte[] test2 = new byte[test.length];
                                m_memory.readBytes(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks byte array writing/reading failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeShorts(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test, 0, test.length);
                                short[] test2 = new short[test.length];
                                m_memory.readShorts(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks short array writing/reading failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeInts(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test, 0, test.length);
                                int[] test2 = new int[test.length];
                                m_memory.readInts(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks int array writing/reading failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeLongs(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test, 0, test.length);
                                long[] test2 = new long[test.length];
                                m_memory.readLongs(ptr, m_memory.getStatus().getMaxBlockSize() - 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks long array writing/reading failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                short v = 0x1122;
                                m_memory.writeShort(ptr, m_memory.getStatus().getMaxBlockSize() + 1, v);
                                short v2 = m_memory.readShort(ptr, m_memory.getStatus().getMaxBlockSize() + 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks short writing/reading to second full block failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                int v = 0x11223344;
                                m_memory.writeInt(ptr, m_memory.getStatus().getMaxBlockSize() + 1, v);
                                int v2 = m_memory.readInt(ptr, m_memory.getStatus().getMaxBlockSize() + 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks int writing/reading to second full block failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                long v = 0x1122334455667788L;
                                m_memory.writeLong(ptr, m_memory.getStatus().getMaxBlockSize() + 1, v);
                                long v2 = m_memory.readLong(ptr, m_memory.getStatus().getMaxBlockSize() + 1);
                                if (v != v2) {
                                    System.out.println("!!! Chained blocks long writing/reading to second full block failed");
                                    System.exit(-1);
                                }
                            }
                            {
                                byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeBytes(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test, 0, test.length);
                                byte[] test2 = new byte[test.length];
                                m_memory.readBytes(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks byte array writing/reading to second block failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeShorts(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test, 0, test.length);
                                short[] test2 = new short[test.length];
                                m_memory.readShorts(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks short array writing/reading to second block failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeInts(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test, 0, test.length);
                                int[] test2 = new int[test.length];
                                m_memory.readInts(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks int array writing/reading to second block failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
                                m_memory.writeLongs(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test, 0, test.length);
                                long[] test2 = new long[test.length];
                                m_memory.readLongs(ptr, m_memory.getStatus().getMaxBlockSize() + 1, test2, 0, test2.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != test2[i]) {
                                        System.out.println("!!! Chained blocks long array writing/reading to second block failed");
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                byte[] test = new byte[size];
                                for (int i = 0; i < test.length; i++) {
                                    test[i] = (byte) i;
                                }
                                m_memory.writeBytes(ptr, 0, test, 0, test.length);
                                m_memory.readBytes(ptr, 0, test, 0, test.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != (byte) i) {
                                        System.out.println("!!! Chained blocks full byte array writing/reading failed: " + i);
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                short[] test = new short[size / Short.BYTES];
                                for (int i = 0; i < test.length; i++) {
                                    test[i] = (short) i;
                                }
                                m_memory.writeShorts(ptr, 0, test, 0, test.length);
                                m_memory.readShorts(ptr, 0, test, 0, test.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != (short) i) {
                                        System.out.println("!!! Chained blocks full short array writing/reading failed: " + i);
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                int[] test = new int[size / Integer.BYTES];
                                for (int i = 0; i < test.length; i++) {
                                    test[i] = i;
                                }
                                m_memory.writeInts(ptr, 0, test, 0, test.length);
                                m_memory.readInts(ptr, 0, test, 0, test.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != i) {
                                        System.out.println("!!! Chained blocks full int array writing/reading failed: " + i);
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                long[] test = new long[size / Long.BYTES];
                                for (int i = 0; i < test.length; i++) {
                                    test[i] = i;
                                }
                                m_memory.writeLongs(ptr, 0, test, 0, test.length);
                                m_memory.readLongs(ptr, 0, test, 0, test.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != i) {
                                        System.out.println("!!! Chained blocks full long array writing/reading failed: " + i);
                                        System.exit(-1);
                                    }
                                }
                            }
                            {
                                long[] test = new long[(size - 8) / Long.BYTES];
                                for (int i = 0; i < test.length; i++) {
                                    test[i] = i;
                                }
                                m_memory.writeInt(ptr, 0, 0xAABBCCDD);
                                m_memory.writeInt(ptr, 4, 0x11223344);
                                m_memory.writeLongs(ptr, 8, test, 0, test.length);
                                if (m_memory.readInt(ptr, 0) != 0xAABBCCDD) {
                                    System.out.println("!!! Chained blocks pseudo vertex int 1 writing/reading failed");
                                    System.exit(-1);
                                }
                                if (m_memory.readInt(ptr, 4) != 0x11223344) {
                                    System.out.println("!!! Chained blocks pseudo vertex int 1 writing/reading failed");
                                    System.exit(-1);
                                }

                                m_memory.readLongs(ptr, 8, test, 0, test.length);
                                for (int i = 0; i < test.length; i++) {
                                    if (test[i] != i) {
                                        System.out.println("!!! Chained blocks pseudo vertex neighbors writing/reading failed: " + i);
                                        System.exit(-1);
                                    }
                                }
                            }
                        }

                    }
                } else if (m_numFreeOperations > 0) {
                    // execute free if blocks allocated
                    if (!m_blocksAlloced.isEmpty()) {
                        Long memoryPtr = m_blocksAlloced.get(0);
                        m_blocksAlloced.remove(0);

                        LOCK.lock();
                        m_memory.free(memoryPtr);
                        LOCK.unlock();
                        if (m_debugPrint) {
                            System.out.println(">>> Freed " + memoryPtr + ":\n" + m_memory);
                        }

                        m_numFreeOperations--;
                    }
                }
            }
        }

        @Override
        public String toString() {
            return "MemoryThread: mallocOperations " + m_numMallocOperations + ", freeOperations: " + m_numFreeOperations + ",  blockSizeMin: " +
                m_blockSizeMin + ", blockSizeMax: " + m_blockSizeMax;
        }
    }
}
