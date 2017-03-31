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

package de.hhu.bsinfo.dxram.run.beineke;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.migration.MigrationService;

/**
 * Test case for the distributed Chunk handling and migrations.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 07.04.2016
 */
public final class MigrationTest {

    // Constants
    private static final int CHUNK_SIZE = 100;
    private static final int NUMBER_OF_CHUNKS = 100;
    private static final int NUMBER_OF_MIGRATIONS = 100;
    private static final int CHUNKS_PER_PUT = 100;
    private static final int CHUNKS_PER_MIGRATION = 1;

    // Constructors

    /**
     * Creates an instance of MigrationTest
     */
    private MigrationTest() {
    }

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        Master.start();
    }

    /**
     * The Master creates a fixed number of chunks and migrates some of them.
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 07.04.202016
     */
    private static final class Master {

        // Constants
        private static final short DEST = (short) -15999;

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
        public static void start() {
            long counter = 0;
            long start;
            DSByteArray[] chunks;

            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.conf");
            final ChunkService chunkService = dxram.getService(ChunkService.class);
            final MigrationService migrationService = dxram.getService(MigrationService.class);

            // Create array of Chunks
            chunks = new DSByteArray[CHUNKS_PER_PUT];
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                chunks[i] = new DSByteArray(CHUNK_SIZE);
                byte[] tmp = "Test!".getBytes();
                System.arraycopy(tmp, 0, chunks[i].getData(), 0, tmp.length);
            }

            start = System.currentTimeMillis();
            while (counter < NUMBER_OF_CHUNKS) {
                // Create new chunks in MemoryManagement
                chunkService.create(chunks);

                // Store them in-memory and replicate them on backups' SSD
                chunkService.put(chunks);

                counter += CHUNKS_PER_PUT;
            }
            System.out.println("Time to create " + NUMBER_OF_CHUNKS + " chunks: " + (System.currentTimeMillis() - start) + " ms");

            // Single migrate
            /*-migrationService.migrate(chunks[0].getID(), DEST);
            System.out.println(new String(chunks[0].getData().array()));*/

            // Multi migrate
            migrationService.migrateRange(chunks[0].getID(), chunks[CHUNKS_PER_PUT - 1].getID(), DEST);

            // Chunk chunk = new Chunk(chunks[0].getID(), chunks[0].getDataSize());
            chunkService.get(chunks);
            for (int i = 0; i < CHUNKS_PER_PUT; i++) {
                System.out.println(new String(chunks[i].getData()));
            }
        }
    }

}
