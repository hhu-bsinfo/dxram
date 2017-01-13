/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.run.beineke;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

/**
 * Third test case for logging. Imitates a test found in http://dx.doi.org/10.1145/2806887.
 * Tests the performance of the log service with reorganization, chunk and network interfaces:
 * Phase 1:
 * - One master creates new chunks until specified utilization is reached. Every CHUNKS_PER_PUT chunks, the chunks are
 * logged by calling put().
 * - Every Chunk is replicated on six backup peers.
 * Phase 2:
 * - Chunks are overwritten to put load on the reorganization. 75 chunks (uniform or zipfian) are updated with every
 * access.
 * - Network bandwidth and cpu load is logged externally.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.01.2016
 */
public final class LogTest3 {

    // Constants
    private static final long BYTES_TO_LOAD = 5196002400L;
    private static final long BYTES_TO_UPDATE = 6000128000L;
    private static final int CHUNK_SIZE = 64;
    private static final int CHUNKS_PER_PUT = 100;
    private static final int CHUNKS_PER_UPDATE = 512;
    private static final int MASTER_THREADS = 1;
    private static final int BENCHMARK_THREADS = 8;

    // Attributes
    private static byte ms_workload;

    // Constructors

    /**
     * Creates an instance of LogTest3
     */
    private LogTest3() {
    }

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        // Workload: 0 -> uniform, 1 -> zipfian
        ms_workload = (byte) 0;

        if (p_arguments.length == 0) {
            System.out.println("Missing program argument: Role (master, benchmark)");
        } else if ("master".equals(p_arguments[0])) {
            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.conf");
            final ChunkService chunkService = dxram.getService(ChunkService.class);
            final BootService bootService = dxram.getService(BootService.class);

            short nodeID = bootService.getNodeID();
            Master currentThread = null;
            for (int i = 0; i < MASTER_THREADS; i++) {
                currentThread = new Master(chunkService, nodeID);
                currentThread.start();
            }
            try {
                currentThread.join();
            } catch (final InterruptedException ignored) {
            }
            System.out.println("All chunks created and replicated.");
        } else if ("benchmark".equals(p_arguments[0])) {
            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.conf");
            final ChunkService chunkService = dxram.getService(ChunkService.class);

            Benchmark currentThread = null;
            for (int i = 0; i < BENCHMARK_THREADS; i++) {
                currentThread = new Benchmark(chunkService);
                currentThread.start();
            }
            try {
                currentThread.join();
            } catch (final InterruptedException ignored) {
            }
            System.out.println("Benchmark finished.");
        }
    }

    /**
     * The Master creates a fixed amount of data.
     *
     * @author Kevin Beineke
     *         22.01.2016
     */
    private static class Master extends Thread {

        // Attributes
        private ChunkService m_chunkService;
        private short m_nodeID;

        // Constructors

        /**
         * Creates an instance of Master
         *
         * @param p_chunkService
         *     the initialized ChunkService
         * @param p_nodeID
         *     the NodeID
         */
        Master(final ChunkService p_chunkService, final short p_nodeID) {
            m_chunkService = p_chunkService;
            m_nodeID = p_nodeID;
        }

        // Methods

        /**
         * Starts the server
         */
        @Override
        public void run() {
            int numberOfRequests;
            long counter = 0;
            long start;
            Chunk[] chunks;
            // Chunk[][] allChunks;
            ByteBuffer data;

            /*
             * Phase 1: Creating chunks
             */
            // Create all chunks
            chunks = new Chunk[CHUNKS_PER_PUT];
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                chunks[i] = new Chunk(CHUNK_SIZE);
                data = chunks[i].getData();
                if (data != null) {
                    data.put("Test!".getBytes());
                }
            }

            numberOfRequests = (int) (BYTES_TO_LOAD / CHUNK_SIZE / CHUNKS_PER_PUT / MASTER_THREADS);
            for (int i = 0; i < numberOfRequests; i++) {
                // Create new chunks in MemoryManagement
                m_chunkService.create(chunks);

                counter += CHUNK_SIZE * CHUNKS_PER_PUT;
                if (counter % (100 * 1000 * 1000) == 0) {
                    System.out.println("Created 100.000.000 bytes. All: " + counter);
                }
            }

            System.out.println("Created all chunks. Start replication now.");

            /*
             * Phase 2: Putting/Replicating chunks
             */
            counter = 0;
            start = System.currentTimeMillis();
            for (int i = 0; i < numberOfRequests; i++) {
                for (int j = 0; j < CHUNKS_PER_PUT; j++) {
                    chunks[j].setID(((long) m_nodeID << 48) + (i * CHUNKS_PER_PUT + j));
                }

                // Store them in-memory and replicate them on backups' SSD
                m_chunkService.put(chunks);

                counter += CHUNK_SIZE * CHUNKS_PER_PUT;
                if (counter % (100 * 1000 * 1000) == 0) {
                    System.out.println("Replicated 100.000.000 bytes. All: " + counter);
                }
            }

            /*-// Create all chunks
            numberOfRequests = (int) (BYTES_TO_LOAD / CHUNK_SIZE / CHUNKS_PER_PUT / MASTER_THREADS);
            allChunks = new Chunk[numberOfRequests][];
            for (int i = 0; i < numberOfRequests; i++) {
                // Create array of Chunks
                allChunks[i] = new Chunk[CHUNKS_PER_PUT];
                for (int j = 0; j < CHUNKS_PER_PUT; j++) {
                    allChunks[i][j] = new Chunk(CHUNK_SIZE);
                    allChunks[i][j].getData().put("Test!".getBytes());
                }

                // Create new chunks in MemoryManagement
                m_chunkService.create(allChunks[i]);

                counter += CHUNK_SIZE * CHUNKS_PER_PUT;
                if (counter % (100 * 1000 * 1000) == 0) {
                    System.out.println("Created 100.000.000 bytes. All: " + counter);
                }
            }

            System.out.println("Created all chunks. Start replication now.");

            /**
             * Phase 2: Putting/Replicating chunks
             */
            /*-counter = 0;
            start = System.currentTimeMillis();
            for (int i = 0; i < numberOfRequests; i++) {
                // Store them in-memory and replicate them on backups' SSD
                m_chunkService.put(allChunks[i]);

                counter += CHUNK_SIZE * CHUNKS_PER_PUT;
                if (counter % (100 * 1000 * 1000) == 0) {
                    System.out.println("Replicated 100.000.000 bytes. All: " + counter);
                }
            }*/

            /*-// Create array of Chunks
            chunks = new Chunk[CHUNKS_PER_PUT];
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                chunks[i] = new Chunk(CHUNK_SIZE);
                chunks[i].getData().put("Test!".getBytes());
            }

            start = System.currentTimeMillis();
            while (counter < BYTES_TO_LOAD / MASTER_THREADS) {
                // Create new chunks in MemoryManagement
                m_chunkService.create(chunks);

                // Store them in-memory and replicate them on backups' SSD
                m_chunkService.put(chunks);

                counter += CHUNK_SIZE * CHUNKS_PER_PUT;
                if (counter % (100 * 1000 * 1000) == 0) {
                    System.out.println("Created 100.000.000 bytes and replicated them. All: " + counter);
                }
            }*/
            System.out.println("Time to create " + BYTES_TO_LOAD / MASTER_THREADS + " bytes of payload: " + (System.currentTimeMillis() - start));
        }
    }

    /**
     * The Benchmark changes (puts) the data of all Masters.
     *
     * @author Kevin Beineke
     *         22.01.2016
     */
    private static class Benchmark extends Thread {

        // Attributes
        private ChunkService m_chunkService;

        // Constructors

        /**
         * Creates an instance of Benchmark
         *
         * @param p_chunkService
         *     the initialized ChunkService
         */
        Benchmark(final ChunkService p_chunkService) {
            m_chunkService = p_chunkService;
        }

        // Methods

        /**
         * Returns a random long
         *
         * @param p_rng
         *     the random number generator
         * @param p_max
         *     the maximum value
         * @return a random long
         */
        static long nextLong(final Random p_rng, final long p_max) {
            long bits;
            long val;

            do {
                bits = p_rng.nextLong() << 1 >>> 1;
                val = bits % p_max;
            } while (bits - val + p_max - 1 < 0L);
            return val;
        }

        /**
         * Starts the client
         */
        @Override
        public void run() {
            /*-final short[] nodeIDs = new short[3];
            nodeIDs[0] = 640;
            nodeIDs[1] = -15807;
            nodeIDs[2] = -14847;*/
            final short[] nodeIDs = new short[1];
            nodeIDs[0] = 960;
            ByteBuffer data;

            long start;
            long counter = 0;
            Chunk[] chunks;

            Random rand;
            FastZipfGenerator zipf;

            // Create array of Chunks
            chunks = new Chunk[CHUNKS_PER_UPDATE];
            for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
                chunks[i] = new Chunk(CHUNK_SIZE);
                data = chunks[i].getData();
                if (data != null) {
                    data.put("Update".getBytes());
                }
            }

            start = System.currentTimeMillis();
            rand = new Random();
            // Send updates to master
            if (ms_workload == 0) {

                while (counter < BYTES_TO_UPDATE / BENCHMARK_THREADS) {
                    /*-long offset = nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1;
                    for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
                        chunks[i].setChunkID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48) + offset + i);
                    }*/
                    long time = System.currentTimeMillis();
                    for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
                        chunks[i].setID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48) + nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1);
                    }

                    m_chunkService.put(chunks);

                    counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
                    if (counter % (10 * 1024 * 1024) == 0) {
                        System.out.println(Thread.currentThread().getName() + ": Updated 10.485.760 bytes with random distribution(left: " +
                            (BYTES_TO_UPDATE / BENCHMARK_THREADS - counter) + ").");
                    }
                }
            } else {
                System.out.println("Initializing ZipfGenerator. This might take a little.");
                zipf = new FastZipfGenerator((int) (BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT + 1), 0.5);

                while (counter < BYTES_TO_UPDATE / BENCHMARK_THREADS) {
                    for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
                        chunks[i].setID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48) + zipf.next());
                    }

                    m_chunkService.put(chunks);

                    counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
                    if (counter % (10 * 1024 * 1024) == 0) {
                        System.out.println(Thread.currentThread().getName() + ": Updated 10.485.760 bytes with zipfian distribution(left: " +
                            (BYTES_TO_UPDATE / BENCHMARK_THREADS - counter) + ").");
                    }
                }
            }
            System.out.println("Time to update " + BYTES_TO_UPDATE + " bytes of payload: " + (System.currentTimeMillis() - start));
        }
    }

    /**
     * A random number generator with zipfian distribution
     * Based on http://diveintodata.org/tag/zipf/
     */
    private static class FastZipfGenerator {
        private Random m_random = new Random(0);
        private NavigableMap<Double, Integer> m_map;

        /**
         * Creates an instance of FastZipfGenerator
         *
         * @param p_size
         *     the number of iterations during generation
         * @param p_skew
         *     the skew
         */
        FastZipfGenerator(final int p_size, final double p_skew) {
            m_map = computeMap(p_size, p_skew);
        }

        /**
         * Computes a map with zipfian distribution
         *
         * @param p_size
         *     the number of iterations during generation
         * @param p_skew
         *     the skew
         * @return the map
         */
        private static NavigableMap<Double, Integer> computeMap(final int p_size, final double p_skew) {
            final NavigableMap<Double, Integer> map = new TreeMap<Double, Integer>();

            double div = 0;
            for (int i = 1; i <= p_size; i++) {
                div += 1 / Math.pow(i, p_skew);
            }

            double sum = 0;
            for (int i = 1; i <= p_size; i++) {
                sum += 1.0d / Math.pow(i, p_skew) / div;
                map.put(sum, i - 1);
            }
            return map;
        }

        /**
         * Returns a random integer with zipfian distribution
         *
         * @return a random integer
         */
        public int next() {
            return m_map.ceilingEntry(m_random.nextDouble()).getValue() + 1;
        }

    }

}
