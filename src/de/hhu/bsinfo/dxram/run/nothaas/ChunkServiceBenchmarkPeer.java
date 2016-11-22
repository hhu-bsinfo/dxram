package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Small test/benchmark to measure execution time of the core methods
 * of ChunkService using the built in StatisticsService.
 * Run this as a peer, start one superpeer.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class ChunkServiceBenchmarkPeer extends DXRAMMain {

    private static final Argument ARG_NUM_CHUNKS = new Argument("numChunks", "1000", true, "Total number of chunks involved in the test");
    private static final Argument ARG_BATCH_SIZE =
        new Argument("batchSize", "10", true, "Split the total number of chunks into a number of batches for get and put operations");
    private static final Argument ARG_CHUNK_SIZE = new Argument("chunkSize", "128", true, "Size of a chunk in bytes");

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
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
        super.registerDefaultProgramArguments(p_arguments);
        p_arguments.setArgument(ARG_NUM_CHUNKS);
        p_arguments.setArgument(ARG_BATCH_SIZE);
        p_arguments.setArgument(ARG_CHUNK_SIZE);
    }

    @Override
    protected int mainApplication(final ArgumentList p_arguments) {
        int numChunks = p_arguments.getArgument(ARG_NUM_CHUNKS).getValue(Integer.class);
        int batchSize = p_arguments.getArgument(ARG_BATCH_SIZE).getValue(Integer.class);
        int chunkSize = p_arguments.getArgument(ARG_CHUNK_SIZE).getValue(Integer.class);

        ChunkService chunkService = getService(ChunkService.class);

        System.out.printf("Running with %d chunks in batches of %d, chunk size %d\n", numChunks, batchSize, chunkSize);

        Chunk[] chunks = new Chunk[batchSize];
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new Chunk(chunkSize);

            // fill with incremental data
            for (int j = 0; j < chunkSize; j++) {
                chunks[i].getData().put((byte) j);
            }
        }

        int iterations = numChunks / batchSize;

        for (int i = 0; i < iterations; i++) {
            if (chunkService.create(chunks) != chunks.length) {
                System.out.println("ERROR: Creating chunks failed.");
                return -1;
            }

            if (chunkService.put(chunks) != chunks.length) {
                System.out.println("ERROR: Putting chunks failed.");
                return -2;
            }

            if (chunkService.get(chunks) != chunks.length) {
                System.out.println("ERROR: Getting chunks failed.");
                return -3;
            }

            // TODO check if data is correct

            if (chunkService.remove(chunks) != chunks.length) {
                System.out.println("ERROR: Removing chunks failed.");
                return -4;
            }
        }

        System.out.println("Done");

        StatisticsService.printStatistics();

        return 0;
    }
}
