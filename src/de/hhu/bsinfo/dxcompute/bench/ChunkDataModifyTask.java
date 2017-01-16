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
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class ChunkDataModifyTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkDataModifyTask.class.getSimpleName());

    private static final int OPERATION_GET = 0;
    private static final int OPERATION_PUT = 1;

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_opCount = 1000;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private int m_operation = OPERATION_GET;

    @Override
    public int execute(final TaskContext p_ctx) {
        if (m_operation < OPERATION_GET || m_operation > OPERATION_PUT) {
            System.out.printf("Invalid operation %d specified\n", m_operation);
            return -1;
        }

        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_opCount, m_numThreads);
        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];
        Chunk[] chunkBatch = new Chunk[m_chunkBatch];

        for (int i = 0; i < chunkBatch.length; i++) {
            chunkBatch[i] = new Chunk();
        }

        ArrayList<Long> chunkRanges = chunkService.getAllLocalChunkIDRanges();

        String opName = "Getting (read only)";
        if (m_operation == OPERATION_PUT) {
            opName = "Putting (write only)";
        }
        System.out.printf("%s %d random chunks in batches of %d chunk(s) with %d thread(s)...\n", opName, m_opCount, m_chunkBatch, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                long batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                long lastBatchRemainder = chunkCountsPerThread[threadIdx] % m_chunkBatch;

                timeStart[threadIdx] = System.nanoTime();
                for (int j = 0; j < batches; j++) {
                    for (Chunk chunk : chunkBatch) {
                        chunk.setID(ChunkTaskUtils.getRandomChunkIdOfRanges(chunkRanges));
                    }

                    if (m_operation == OPERATION_GET) {
                        chunkService.get(chunkBatch);
                    } else {
                        chunkService.put(chunkBatch);
                    }
                }
                if (lastBatchRemainder > 0) {
                    for (int k = 0; k < lastBatchRemainder; k++) {
                        chunkBatch[k].setID(ChunkTaskUtils.getRandomChunkIdOfRanges(chunkRanges));
                    }

                    if (m_operation == OPERATION_GET) {
                        chunkService.get(chunkBatch, 0, (int) lastBatchRemainder);
                    } else {
                        chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, chunkBatch, 0, (int) lastBatchRemainder);
                    }
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
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_opCount = p_importer.readInt();
        m_chunkBatch = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3;
    }
}
