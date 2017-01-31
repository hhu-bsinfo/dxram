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

package de.hhu.bsinfo.dxcompute.bench;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkIDRangeUtils;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

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
    private long m_opCount = 100000;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private boolean m_writeContentsAndVerify = false;
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

        ArrayList<Long> allChunkRanges = ChunkTaskUtils.getChunkRangesForTestPattern(m_pattern / 2, p_ctx, chunkService);
        long totalChunkCount = ChunkIDRangeUtils.countTotalChunksOfRanges(allChunkRanges);
        long[] chunkCountsPerThread = ChunkIDRangeUtils.distributeChunkCountsToThreads(totalChunkCount, m_numThreads);
        ArrayList<Long>[] chunkRangesPerThread = ChunkIDRangeUtils.distributeChunkRangesToThreads(chunkCountsPerThread, allChunkRanges);
        long[] operationsPerThread = ChunkIDRangeUtils.distributeChunkCountsToThreads(m_opCount, m_numThreads);

        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];

        System.out.printf("Modifying %d random chunks (pattern %d) in batches of %d chunk(s) with %d thread(s)...\n", m_opCount, m_pattern, m_chunkBatch,
            m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                long[] chunkIds = new long[m_chunkBatch];
                long operations = operationsPerThread[threadIdx] / m_chunkBatch;
                ArrayList<Long> chunkRanges = chunkRangesPerThread[threadIdx];

                if (operationsPerThread[threadIdx] % m_chunkBatch > 0) {
                    operations++;
                }

                // happens if no chunks were created
                if (!chunkRanges.isEmpty()) {
                    timeStart[threadIdx] = System.nanoTime();

                    while (operations > 0) {
                        int batchCnt = 0;
                        while (operations > 0 && batchCnt < chunkIds.length) {
                            chunkIds[batchCnt] = ChunkIDRangeUtils.getRandomChunkIdOfRanges(chunkRanges);
                            operations--;
                            batchCnt++;
                        }

                        Chunk[] chunks = chunkService.get(chunkIds);

                        if (m_writeContentsAndVerify) {
                            for (int j = 0; j < chunks.length; j++) {
                                ByteBuffer buffer = chunks[j].getData();

                                if (buffer == null) {
                                    throw new IllegalStateException("Buffer of valid chunk null");
                                }

                                buffer.position(0);
                                for (int k = 0; k < buffer.capacity(); k++) {
                                    buffer.put((byte) k);
                                }
                            }
                        }

                        if (doPut) {
                            chunkService.put(chunks);

                            if (m_writeContentsAndVerify) {
                                Chunk[] chunksToVerify = chunkService.get(chunkIds);

                                for (int j = 0; j < chunksToVerify.length; j++) {
                                    ByteBuffer buffer = chunksToVerify[j].getData();

                                    if (buffer == null) {
                                        throw new IllegalStateException("Buffer of valid chunk null");
                                    }

                                    buffer.position(0);
                                    for (int k = 0; k < buffer.capacity(); k++) {
                                        byte b = buffer.get();
                                        if (b != (byte) k) {
                                            LOGGER
                                                .error("Contents of chunk 0x%16X are not matching written contents: 0x%X != 0x%X", chunksToVerify[j].getID(), b,
                                                    k);
                                        }
                                    }
                                }
                            }
                        }
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
        p_exporter.writeLong(m_opCount);
        p_exporter.writeInt(m_chunkBatch);
        p_exporter.writeBoolean(m_writeContentsAndVerify);
        p_exporter.writeInt(m_pattern);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_opCount = p_importer.readLong();
        m_chunkBatch = p_importer.readInt();
        m_writeContentsAndVerify = p_importer.readBoolean();
        m_pattern = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3 + ObjectSizeUtil.sizeofBoolean() + Long.BYTES;
    }
}
