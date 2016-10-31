package de.hhu.bsinfo.dxram.run.beineke;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/**
 * Test case for the distributed Chunk handling and nameservice.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 11.04.2016
 */
public final class NameserviceTest {

    // Constants
    protected static final int CHUNK_SIZE = 100;
    protected static final int NUMBER_OF_CHUNKS = 1000;
    protected static final int CHUNKS_PER_PUT = 100;

    // Constructors

    /**
     * Creates an instance of NameserviceTest
     */
    private NameserviceTest() {
    }

    /**
     * Program entry point
     *
     * @param p_arguments
     *         The program arguments
     */
    public static void main(final String[] p_arguments) {
        new Master().start();
    }

    /**
     * The Master creates a fixed number of chunks and registers them in name service.
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 07.04.202016
     */
    private static class Master {

        // Constructors

        /**
         * Creates an instance of Master
         */
        Master() {
        }

        // Methods

        /**
         * Starts the Master
         */
        public void start() {
            int j = 0;
            long counter = 0;
            long start;
            Chunk[] chunks;

            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.conf");
            final ChunkService chunkService = dxram.getService(ChunkService.class);
            final NameserviceService nameService = dxram.getService(NameserviceService.class);

            // Create array of Chunks
            chunks = new Chunk[CHUNKS_PER_PUT];
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                chunks[i] = new Chunk(CHUNK_SIZE);
                chunks[i].getData().put("Test!".getBytes());
            }

            start = System.currentTimeMillis();
            while (counter < NUMBER_OF_CHUNKS) {
                // Create new chunks in MemoryManagement
                chunkService.create(chunks);

                // Store them in-memory and replicate them on backups' SSD
                chunkService.put(chunks);

                counter += CHUNKS_PER_PUT;

                for (Chunk c : chunks) {
                    nameService.register(c, (j++) + "");
                }
            }
            System.out.println("Time to create " + NUMBER_OF_CHUNKS + " chunks: " + (System.currentTimeMillis() - start) + " ms");

            start = System.currentTimeMillis();
            for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
                nameService.getChunkID(i + "", -1);
            }
            System.out.println("Time to get ChunkIDs of " + NUMBER_OF_CHUNKS + " chunks: " + (System.currentTimeMillis() - start) + " ms");

            System.out.println("Number of entries: " + nameService.getEntryCount());
        }
    }

}
