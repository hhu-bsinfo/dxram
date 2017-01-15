package de.hhu.bsinfo.dxcompute.bench;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ChunkCreateTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkCreateTask.class.getSimpleName());

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_chunkCount = 1000;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private StorageUnit m_chunkSizeBytesBegin = new StorageUnit(16, StorageUnit.BYTE);
    @Expose
    private StorageUnit m_chunkSizeBytesEnd = new StorageUnit(512, StorageUnit.BYTE);

    public ChunkCreateTask() {

    }

    public ChunkCreateTask(final int p_numThreads, final int p_chunkCount, final StorageUnit p_chunkSizeBytesBegin, final StorageUnit p_chunkSizeBytesEnd) {
        m_numThreads = p_numThreads;
        m_chunkCount = p_chunkCount;
        m_chunkSizeBytesBegin = p_chunkSizeBytesBegin;
        m_chunkSizeBytesEnd = p_chunkSizeBytesEnd;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_chunkCount, m_numThreads);
        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];

        System.out.printf("Creating %d chunks in batches of %d chunk(s) of random sizes between %s and %s with %d thread(s)...\n",
                m_chunkCount, m_chunkBatch, m_chunkSizeBytesBegin, m_chunkSizeBytesEnd, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                long batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                long lastBatchRemainder = chunkCountsPerThread[threadIdx] % m_chunkBatch;

                timeStart[threadIdx] = System.nanoTime();
                for (int j = 0; j < batches; j++) {
                    int size = ChunkTaskUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                    chunkService.create(size, m_chunkBatch);
                }
                if (lastBatchRemainder > 0) {
                    int size = ChunkTaskUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                    chunkService.create(size, (int) lastBatchRemainder);
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

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        System.out.printf("Throughput: %f chunks/sec\n", 1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_chunkCount));

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_chunkCount);
        p_exporter.writeInt(m_chunkBatch);
        p_exporter.exportObject(m_chunkSizeBytesBegin);
        p_exporter.exportObject(m_chunkSizeBytesEnd);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_chunkCount = p_importer.readInt();
        m_chunkBatch = p_importer.readInt();
        m_chunkSizeBytesBegin = new StorageUnit();
        p_importer.importObject(m_chunkSizeBytesBegin);
        m_chunkSizeBytesEnd = new StorageUnit();
        p_importer.importObject(m_chunkSizeBytesEnd);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3 + m_chunkSizeBytesBegin.sizeofObject() + m_chunkSizeBytesEnd.sizeofObject();
    }
}
