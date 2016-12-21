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

package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

/**
 * Small test/benchmark to measure execution time of the core methods
 * of ChunkService using the built in StatisticsService.
 * Run this as a peer, start one superpeer.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class ChunkServiceBenchmarkPeer extends DXRAMMain {

    /**
     * Constructor
     */
    private ChunkServiceBenchmarkPeer() {
        super("ChunkServiceBenchmarkPeer");
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        DXRAMMain main = new ChunkServiceBenchmarkPeer();
        main.run(p_args);
    }

    @Override
    protected int mainApplication(final String[] p_args) {
        if (p_args.length < 3) {
            System.out.println("Usage: " + getClass().getSimpleName() + " [numChunks] [batchSize] [chunkSize]");
            return -1;
        }

        int numChunks = Integer.parseInt(p_args[0]);
        int batchSize = Integer.parseInt(p_args[1]);
        int chunkSize = Integer.parseInt(p_args[2]);

        System.out.printf("Running with %d chunks in batches of %d, chunk size %d\n", numChunks, batchSize, chunkSize);

        System.out.println(">>> Test 1");
        test1(numChunks, batchSize, chunkSize);
        StatisticsService.printStatistics();
        System.out.println("=======================================");
        StatisticsService.resetStatistics();

        System.out.println(">>> Test 2");
        test2(numChunks, batchSize, chunkSize);
        StatisticsService.printStatistics();
        System.out.println("=======================================");
        StatisticsService.resetStatistics();

        System.out.println("Done");

        return 0;
    }

    private void test1(final int p_numChunks, final int p_batchSize, final int p_chunkSize) {
        ChunkService chunkService = getService(ChunkService.class);

        Chunk[] chunks = new Chunk[p_batchSize];
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new Chunk(p_chunkSize);

            // fill with incremental data
            for (int j = 0; j < p_chunkSize; j++) {
                chunks[i].getData().put((byte) j);
            }
        }

        int iterations = p_numChunks / p_batchSize;

        for (int i = 0; i < iterations; i++) {
            if (chunkService.create(chunks) != chunks.length) {
                System.out.println("ERROR: Creating chunks failed.");
                System.exit(-1);
            }

            if (chunkService.put(chunks) != chunks.length) {
                System.out.println("ERROR: Putting chunks failed.");
                System.exit(-2);
            }

            if (chunkService.get(chunks) != chunks.length) {
                System.out.println("ERROR: Getting chunks failed.");
                System.exit(-3);
            }

            // TODO check if data is correct

            if (chunkService.remove(chunks) != chunks.length) {
                System.out.println("ERROR: Removing chunks failed.");
                System.exit(-4);
            }
        }
    }

    private void test2(final int p_numChunks, final int p_batchSize, final int p_chunkSize) {
        ChunkService chunkService = getService(ChunkService.class);

        int iterations = p_numChunks / p_batchSize;

        for (int i = 0; i < iterations; i++) {
            long[] chunkdIDs = chunkService.create(p_chunkSize, p_batchSize);

            if (chunkdIDs == null) {
                System.out.println("ERROR: Creating chunks failed.");
                System.exit(-1);
            }

            if (chunkService.remove(chunkdIDs) != chunkdIDs.length) {
                System.out.println("ERROR: Removing chunks failed.");
                System.exit(-2);
            }
        }
    }
}
