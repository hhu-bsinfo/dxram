package de.hhu.bsinfo.dxcompute.bench;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class ChunkDataModifyTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkDataModifyTask.class.getSimpleName());

    private static final int PATTERN_GET_LOCAL = 0;
    private static final int PATTERN_GET_PUT_LOCAL = 1;
    private static final int PATTERN_GET_REMOTE_ONLY_SUCCESSOR = 2;
    private static final int PATTERN_GET_PUT_REMOTE_ONLY_SUCCESSOR = 3;
    private static final int PATTERN_GET_REMOTE_ONLY_RANDOM = 4;
    private static final int PATTERN_GET_PUT_REMOTE_ONLY_RANDOM = 5;
    private static final int PATTERN_GET_REMOTE_LOCAL_MIXED_RANDOM = 6;
    private static final int PATTERN_GET_PUT_REMOTE_LOCAL_MIXED_RANDOM = 7;

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_opCount = 100000;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private int m_pattern = PATTERN_GET_LOCAL;

    @Override
    public int execute(final TaskContext p_ctx) {
        boolean remote = m_pattern > PATTERN_GET_PUT_LOCAL;
        boolean doPut = m_pattern % 2 == 1;

        if (remote && p_ctx.getCtxData().getSlaveNodeIds().length < 2) {
            System.out.println("Not enough slaves (min 2) to execute this task");
            return -1;
        }

        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        long activeChunkCount = chunkService.getStatus().getNumberOfActiveChunks();
        // don't modify the index chunk
        activeChunkCount -= 1;

        ArrayList<Long> allChunkRanges = ChunkTaskUtils.getChunkRangesForTestPattern(m_pattern / 2, p_ctx, chunkService);
        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(activeChunkCount, m_numThreads);
        ArrayList<Long>[] chunkRangesPerThread = ChunkTaskUtils.distributeChunkRangesToThreads(chunkCountsPerThread, allChunkRanges);

        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];

        System.out.printf("Modifying %d random chunks (pattern %d) in batches of %d chunk(s) with %d thread(s)...\n", m_opCount, m_pattern, m_chunkBatch,
            m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                long[] chunkIds = new long[m_chunkBatch];
                long batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                ArrayList<Long> chunkRanges = chunkRangesPerThread[threadIdx];

                if (chunkCountsPerThread[threadIdx] % m_chunkBatch > 0) {
                    batches++;
                }

                // happens if no chunks were created
                if (!chunkRanges.isEmpty()) {
                    int rangeIdx = 0;
                    long rangeStart = chunkRanges.get(rangeIdx * 2);
                    long rangeEnd = chunkRanges.get(rangeIdx * 2 + 1);
                    long batchChunkCount = m_chunkBatch;

                    timeStart[threadIdx] = System.nanoTime();

                    int fillCount = 0;
                    while (batches > 0) {
                        long chunksInRange = ChunkID.getLocalID(rangeEnd) - ChunkID.getLocalID(rangeStart) + 1;

                        if (chunksInRange >= batchChunkCount) {
                            for (int j = fillCount; j < chunkIds.length; j++) {
                                chunkIds[j] = rangeStart++;
                            }

                            fillCount = 0;
                        } else {
                            // chunksInRange < m_chunkBatch

                            for (int j = fillCount; j < fillCount + chunksInRange; j++) {
                                chunkIds[j] = rangeStart++;
                            }

                            fillCount += chunksInRange;

                            rangeIdx++;
                            if (rangeIdx * 2 < chunkRanges.size()) {
                                rangeStart = chunkRanges.get(rangeIdx * 2);
                                rangeEnd = chunkRanges.get(rangeIdx * 2 + 1);
                                continue;
                            }

                            // invalidate spare chunk ids
                            for (int j = fillCount; j < chunkIds.length; j++) {
                                chunkIds[j] = ChunkID.INVALID_ID;
                            }

                            fillCount = 0;
                        }

                        Chunk[] chunks = chunkService.get(chunkIds);

                        if (doPut) {
                            chunkService.put(chunks);
                        }

                        batches--;
                    }

                    timeEnd[threadIdx] = System.nanoTime();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        boolean threadJoinFailed = false;
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                LOGGER.error("Joining thread failed", e);
                threadJoinFailed = true;
            }
        }

        if (threadJoinFailed) {
            return -2;
        }

        System.out.print("Times per thread:");
        for (int i = 0; i < m_numThreads; i++) {
            System.out.printf("\nThread-%d: %f sec", i, (timeEnd[i] - timeStart[i]) / 1000.0 / 1000.0 / 1000.0);
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_numThreads; i++) {
            long time = timeEnd[i] - timeStart[i];
            if (time > totalTime) {
                totalTime = time;
            }
        }

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        System.out.printf("Throughput: %f chunks/sec\n", 1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_opCount));

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_opCount);
        p_exporter.writeInt(m_chunkBatch);
        p_exporter.writeInt(m_pattern);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_opCount = p_importer.readInt();
        m_chunkBatch = p_importer.readInt();
        m_pattern = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 4;
    }
}
