package de.hhu.bsinfo.dxram.run.beineke;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

/**
 * Second test case for logging
 * Tests the performance of the log (without reorganization), chunk and network interfaces:
 * - One master creates new chunks until it runs out of memory. Every CHUNKS_PER_PUT chunks, the chunks are logged by calling put().
 * - Every Chunk is replicated on three backup peers.
 * - Network bandwidth is logged externally.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.01.2016
 */
public final class LogTest2 {

    // Constants
    private static final int THREADS = 1;
    private static final int CHUNK_SIZE = 64;
    private static final int CHUNKS_PER_PUT = 100;

    // Constructors

    /**
     * Creates an instance of LogTest2
     */
    private LogTest2() {
    }

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        // Initialize DXRAM
        final DXRAM dxram = new DXRAM();
        dxram.initialize("config/dxram.conf");
        final ChunkService chunkService = dxram.getService(ChunkService.class);

        Master currentThread = null;
        for (int i = 0; i < THREADS; i++) {
            currentThread = new Master(chunkService);
            currentThread.start();
        }
        try {
            currentThread.join();
        } catch (final InterruptedException ignored) {
        }
    }

    /**
     * The Master constantly creates new chunks and sends them automatically to backup peers if backup is enabled.
     *
     * @author Kevin Beineke
     *         19.01.2016
     */
    private static class Master extends Thread {

        // Attributes
        private ChunkService m_chunkService;

        // Constructors

        /**
         * Creates an instance of Master
         *
         * @param p_chunkService
         *     the initialized ChunkService
         */
        Master(final ChunkService p_chunkService) {
            m_chunkService = p_chunkService;
        }

        // Methods

        /**
         * Starts the Master
         */
        @Override
        public void run() {
            long counter = 0;
            long start;
            Chunk[] chunks;
            ByteBuffer data;

            // Create array of Chunks
            chunks = new Chunk[CHUNKS_PER_PUT];
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                chunks[i] = new Chunk(CHUNK_SIZE);
                data = chunks[i].getData();
                if (data != null) {
                    data.put("Test!".getBytes());
                }
            }

            start = System.currentTimeMillis();
            while (counter < 10737418240L / THREADS) {
                // Create new chunks in MemoryManagement
                m_chunkService.create(chunks);

                // Store them in-memory and replicate them on backups' SSD
                m_chunkService.put(chunks);

                counter += CHUNKS_PER_PUT * CHUNK_SIZE;
                // System.out.println("Created " + CHUNKS_PER_PUT + " chunks and replicated them.");
            }
            System.out.println("Time to distribute 3GB payload: " + (System.currentTimeMillis() - start) + " ms");
        }
    }

}
