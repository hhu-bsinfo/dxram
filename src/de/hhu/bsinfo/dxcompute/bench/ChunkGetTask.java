package de.hhu.bsinfo.dxcompute.bench;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;

public class ChunkGetTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkGetTask.class.getSimpleName());

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_getCount = 1000;
    @Expose
    private int m_chunkBatch = 10;

    @Override
    public int execute(TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        int[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_getCount, m_numThreads);
        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];
        Chunk[] chunkBatch = new Chunk[m_chunkBatch];

        for (int i = 0; i < chunkBatch.length; i++) {
            chunkBatch[i] = new Chunk();
        }

        ArrayList<Long> chunkRanges = chunkService.getAllLocalChunkIDRanges();

        System.out.printf("Getting (read only) %d random chunks in batches of %d chunk(s) with %d thread(s)...\n",
                m_getCount, m_chunkBatch, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                int batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                int lastBatchRemainder = chunkCountsPerThread[threadIdx] % m_chunkBatch;

                timeStart[threadIdx] = System.nanoTime();
                for (int j = 0; j < batches; j++) {
                    for (Chunk chunk : chunkBatch) {
                        chunk.setID(ChunkTaskUtils.getRandomChunkIdOfRanges(chunkRanges));
                    }

                    chunkService.get(chunkBatch);
                }
                if (lastBatchRemainder > 0) {
                    for (int k = 0; k < lastBatchRemainder; k++) {
                        chunkBatch[k].setID(ChunkTaskUtils.getRandomChunkIdOfRanges(chunkRanges));
                    }

                    chunkService.get(chunkBatch, 0, lastBatchRemainder);
                }
                timeEnd[threadIdx] = System.nanoTime();
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
            return -1;
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

        System.out.printf("Throughput: %f chunks/sec", 1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_getCount));

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_getCount);
        p_exporter.writeInt(m_chunkBatch);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_getCount = p_importer.readInt();
        m_chunkBatch = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3;
    }
}
