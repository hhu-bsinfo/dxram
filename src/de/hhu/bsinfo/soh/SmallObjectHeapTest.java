package de.hhu.bsinfo.soh;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests concurrent allocation and freeing of memory
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class SmallObjectHeapTest {
    private static final Lock LOCK = new ReentrantLock(false);

    private SmallObjectHeap m_memory;
    private int m_numThreads = -1;
    private int m_numOperations = -1;
    private float m_mallocFreeRatio;
    private int m_blockSizeMin = -1;
    private int m_blockSizeMax = -1;
    private boolean m_debugPrint;

    /**
     * Constructor
     *
     * @param p_memorySize
     *     Total raw memory size in bytes.
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
    private SmallObjectHeapTest(final long p_memorySize, final int p_numThreads, final int p_numOperations, final float p_mallocFreeRatio,
        final int p_blockSizeMin, final int p_blockSizeMax, final boolean p_debugPrint) {
        assert p_memorySize > 0;
        assert p_numThreads > 0;
        assert p_numOperations > 0;
        assert p_mallocFreeRatio >= 0.5;
        assert p_blockSizeMin > 0;
        assert p_blockSizeMax > 0;
        assert p_blockSizeMax > p_blockSizeMin;

        // m_memory = new SmallObjectHeap(new StorageRandomAccessFile(new File("memory.raw")), p_memorySize);
        m_memory = new SmallObjectHeap(new StorageUnsafeMemory(), p_memorySize);

        m_numThreads = p_numThreads;
        m_numOperations = p_numOperations;
        m_mallocFreeRatio = p_mallocFreeRatio;
        m_blockSizeMin = p_blockSizeMin;
        m_blockSizeMax = p_blockSizeMax;
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
            MemoryThread memThread = new MemoryThread(m_memory, m_numOperations, m_mallocFreeRatio, m_blockSizeMin, m_blockSizeMax, m_debugPrint);
            submittedTasks.add(executor.submit(memThread));
        }

        System.out.println("Waiting for workers to finish...");

        for (Future<?> future : submittedTasks) {
            try {
                future.get();
            } catch (final ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        //................File file = new File("rawMemory.dump");
        //................if (file.exists()) {
        //....................file.delete();
        //....................try {
        //........................file.createNewFile();
        //....................} catch (IOException e) {
        //........................e.printStackTrace();
        //....................}
        //................}
        //................m_memory.dump(file, 0, 12 * 1024 * 1024);

        System.out.println("All workers finished.");

        System.out.println("Final memory status:\n" + m_memory);

        HeapWalker.Results results = HeapWalker.walk(m_memory);
        System.out.println(results);

        executor.shutdown();
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Command line arguments
     */
    public static void main(final String[] p_args) {
        if (p_args.length >= 1 && p_args.length < 7) {
            System.out.println("Usage: RawMemoryTest <memorySize> <numThreads> <numOperations> <mallocFreeRatio> <blockSizeMin> <blockSizeMax> <debugPrint>");
            return;
        }

        if (p_args.length == 0) {
            runTests();
        } else {
            long memorySize = Long.parseLong(p_args[0]);
            int numThreads = Integer.parseInt(p_args[1]);
            int numOperations = Integer.parseInt(p_args[2]);
            float mallocFreeRatio = Float.parseFloat(p_args[3]);
            int blockSizeMin = Integer.parseInt(p_args[4]);
            int blockSizeMax = Integer.parseInt(p_args[5]);
            boolean debugPrint = Boolean.parseBoolean(p_args[6]);

            runTest(0, memorySize, numThreads, numOperations, mallocFreeRatio, blockSizeMin, blockSizeMax, debugPrint);
        }
    }

    /**
     * List of hardcoded tests to ensure everything's working
     */
    private static void runTests() {
        // single malloc
        runTest(0, 1024, 1, 1, 1.0f, 16, 16, false);
        // a few more mallocs
        runTest(1, 1024, 1, 3, 1.0f, 16, 16, false);
        // single malloc + free
        runTest(2, 1024, 1, 2, 0.5f, 16, 16, false);
        // multiple malloc + free
        runTest(3, 1024, 1, 6, 0.5f, 16, 16, false);

        // some greater tests
        runTest(4, 1024 * 1024, 1, 10, 0.5f, 16, 1024, false);
        runTest(5, 1024 * 1024 * 1024, 1, 100, 0.5f, 16, 1024 * 1024, false);
        runTest(6, 1024 * 1024 * 1024, 1, 1000, 0.5f, 16, 1024 * 1024, false);
        runTest(7, 1024 * 1024 * 1024, 1, 10000, 0.5f, 16, 1024 * 1024, false);
        runTest(8, 1024 * 1024 * 1024, 1, 100000, 0.5f, 16, 1024 * 1024, false);
        runTest(8, 1024 * 1024 * 1024, 1, 1000000, 0.5f, 16, 1024 * 1024, false);

        // block chaining
        runTest(9, 1024 * 1024 * 32, 1, 1, 1.0f, 1024 * 1024 * 9, 1024 * 1024 * 16, false);
        runTest(10, 1024 * 1024 * 128, 1, 6, 0.5f, 1024 * 1024 * 9, 1024 * 1024 * 16, false);
        runTest(11, 1024 * 1024 * 1024, 1, 100, 0.5f, 1024 * 1024 * 9, 1024 * 1024 * 16, false);

        // huge test (32gb ram necessary!)
        runTest(12, 1024 * 1024 * 1024 * 32L, 1, 10000, 0.5f, 16, 1024 * 1024 * 64, false);
    }

    private static void runTest(int testId, long memorySize, int numThreads, int numOperations, float mallocFreeRatio, int blockSizeMin, int blockSizeMax,
        boolean debugPrint) {

        System.out.println("===============================================================");
        System.out.println("Initializing RawMemory test (" + testId + ")...");
        SmallObjectHeapTest test = new SmallObjectHeapTest(memorySize, numThreads, numOperations, mallocFreeRatio, blockSizeMin, blockSizeMax, debugPrint);
        System.out.println("Running test (" + testId + ")...");
        long timeStart = System.nanoTime();
        test.run();
        long time = System.nanoTime() - timeStart;
        System.out.println("Test (" + testId + ") done: " + time / 1000.0 / 1000.0 + " ms");
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
            final int p_blockSizeMax, final boolean p_debugPrint) {
            assert m_blockSizeMin > 0;
            assert m_blockSizeMax > 0;
            assert m_mallocFreeRatio > 0;

            m_memory = p_rawMemory;
            m_blockSizeMin = p_blockSizeMin;
            m_blockSizeMax = p_blockSizeMax;
            m_mallocFreeRatio = p_mallocFreeRatio;
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
                    // execute alloc
                    int size = 0;
                    while (size <= 0) {
                        size = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin)) + m_blockSizeMin;
                    }

                    long ptr;

                    LOCK.lock();
                    ptr = m_memory.malloc(size);
                    LOCK.unlock();
                    if (m_debugPrint) {
                        System.out.println(">>> Allocated " + size + ":\n" + m_memory);
                    }
                    m_blocksAlloced.add(ptr);
                    m_memory.set(ptr, size, (byte) 0xBB);

                    // test writing/reading across two blocks
                    if (size > SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK) {
                        {
                            if (m_memory.getSizeBlock(ptr) <= SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK) {
                                System.out.println("!!! Chained blocks getting size failed");
                                System.exit(-1);
                            }
                        }
                        {
                            short v = 0x1122;
                            m_memory.writeShort(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, v);
                            short v2 = m_memory.readShort(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks short writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            int v = 0xAABBCCDD;
                            m_memory.writeInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, v);
                            int v2 = m_memory.readInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks int (1) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            int v = 0xAABBCCDD;
                            m_memory.writeInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 2, v);
                            int v2 = m_memory.readInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 2);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks int (2) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            int v = 0xAABBCCDD;
                            m_memory.writeInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 3, v);
                            int v2 = m_memory.readInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 3);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks int (3) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (1) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 2, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 2);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (2) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 3, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 3);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (3) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 4, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 4);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (4) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 5, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 5);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (5) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 6, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 6);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (6) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 7, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 7);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (7) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 8, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 8);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long (8) writing/reading failed");
                                System.exit(-1);
                            }
                        }
                        {
                            byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeBytes(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test, 0, test.length);
                            byte[] test2 = new byte[test.length];
                            m_memory.readBytes(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks byte array writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeShorts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test, 0, test.length);
                            short[] test2 = new short[test.length];
                            m_memory.readShorts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks short array writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeInts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test, 0, test.length);
                            int[] test2 = new int[test.length];
                            m_memory.readInts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks int array writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeLongs(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test, 0, test.length);
                            long[] test2 = new long[test.length];
                            m_memory.readLongs(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks long array writing/reading failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            short v = 0x1122;
                            m_memory.writeShort(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, v);
                            short v2 = m_memory.readShort(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks short writing/reading to second full block failed");
                                System.exit(-1);
                            }
                        }
                        {
                            int v = 0x11223344;
                            m_memory.writeInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, v);
                            int v2 = m_memory.readInt(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks int writing/reading to second full block failed");
                                System.exit(-1);
                            }
                        }
                        {
                            long v = 0x1122334455667788L;
                            m_memory.writeLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, v);
                            long v2 = m_memory.readLong(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1);
                            if (v != v2) {
                                System.out.println("!!! Chained blocks long writing/reading to second full block failed");
                                System.exit(-1);
                            }
                        }
                        {
                            byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeBytes(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test, 0, test.length);
                            byte[] test2 = new byte[test.length];
                            m_memory.readBytes(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks byte array writing/reading to second block failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeShorts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test, 0, test.length);
                            short[] test2 = new short[test.length];
                            m_memory.readShorts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks short array writing/reading to second block failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeInts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test, 0, test.length);
                            int[] test2 = new int[test.length];
                            m_memory.readInts(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0, test2.length);
                            for (int i = 0; i < test.length; i++) {
                                if (test[i] != test2[i]) {
                                    System.out.println("!!! Chained blocks int array writing/reading to second block failed");
                                    System.exit(-1);
                                }
                            }
                        }
                        {
                            long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
                            m_memory.writeLongs(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test, 0, test.length);
                            long[] test2 = new long[test.length];
                            m_memory.readLongs(ptr, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0, test2.length);
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

                    m_numMallocOperations--;
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
