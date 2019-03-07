/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk.bench;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.Stopwatch;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Task to remove chunks from a node using different patterns
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 25.01.2017
 */
public class ChunkRemoveAllTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkRemoveAllTask.class);

    private static final int PATTERN_LOCAL_ONLY = 0;
    private static final int PATTERN_REMOTE_ONLY_SUCCESSOR = 1;
    private static final int PATTERN_REMOTE_ONLY_RANDOM = 2;
    private static final int PATTERN_REMOTE_LOCAL_MIXED_RANDOM = 3;

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private int m_pattern = PATTERN_LOCAL_ONLY;

    public ChunkRemoveAllTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {
        boolean remote = m_pattern > PATTERN_LOCAL_ONLY;

        if (remote && p_ctx.getCtxData().getSlaveNodeIds().length < 2) {
            System.out.println("Not enough slaves (min 2) to execute this task");
            return -1;
        }

        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        long activeChunkCount = chunkService.status().getStatus().getLIDStoreStatus().getCurrentLIDCounter();
        // don't remove the index chunk
        activeChunkCount -= 1;

        ChunkIDRanges allChunkRanges = ChunkTaskUtils.getChunkRangesForTestPattern(m_pattern, p_ctx, chunkService);
        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(activeChunkCount, m_numThreads);
        ChunkIDRanges[] chunkRangesPerThread = ChunkTaskUtils.distributeChunkRangesToThreads(chunkCountsPerThread,
                allChunkRanges);

        Thread[] threads = new Thread[m_numThreads];
        Stopwatch[] time = new Stopwatch[m_numThreads];
        for (int i = 0; i < time.length; i++) {
            time[i] = new Stopwatch();
        }

        System.out.printf(
                "Removing all (pattern %d) active chunks (total %d) in batches of %d chunk(s) with %d thread(s)...\n",
                m_pattern, activeChunkCount,
                m_chunkBatch, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                long[] chunkIds = new long[m_chunkBatch];
                long batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                long lastBatchRemainder = chunkCountsPerThread[threadIdx] % m_chunkBatch;
                ChunkIDRanges chunkRanges = chunkRangesPerThread[threadIdx];

                if (lastBatchRemainder > 0) {
                    batches++;
                }

                // happens if no chunks were created
                if (!chunkRanges.isEmpty()) {
                    int rangeIdx = 0;
                    long rangeStart = chunkRanges.getRangeStart(rangeIdx);
                    long rangeEnd = chunkRanges.getRangeEnd(rangeIdx);
                    long batchChunkCount = m_chunkBatch;

                    while (batches > 0) {
                        int fillCount = 0;
                        long chunksInRange = ChunkID.getLocalID(rangeEnd) - ChunkID.getLocalID(rangeStart) + 1;

                        if (chunksInRange >= batchChunkCount) {
                            for (int j = fillCount; j < batchChunkCount; j++) {
                                chunkIds[j] = rangeStart + j;
                            }
                        } else {
                            // chunksInRange < m_chunkBatch

                            for (int j = fillCount; j < chunksInRange; j++) {
                                chunkIds[j] = rangeStart + j;
                                fillCount++;
                            }

                            rangeIdx++;
                            if (rangeIdx * 2 < chunkRanges.size()) {
                                rangeStart = chunkRanges.getRangeStart(rangeIdx);
                                rangeEnd = chunkRanges.getRangeEnd(rangeIdx);
                                continue;
                            } else {
                                // invalidate spare chunk ids
                                for (int j = fillCount; j < chunkIds.length; j++) {
                                    chunkIds[j] = ChunkID.INVALID_ID;
                                }

                                batches = 0;
                            }
                        }

                        time[threadIdx].start();
                        int ret = chunkService.remove().remove(chunkIds);
                        time[threadIdx].stopAndAccumulate();

                        if (ret != chunkIds.length) {
                            // count valid chunks, first
                            int valid = 0;
                            for (int k = 0; k < chunkIds.length; k++) {
                                if (chunkIds[k] != ChunkID.INVALID_ID) {
                                    valid++;
                                }
                            }

                            if (ret != valid) {
                                LOGGER.error("Removing one or multiple chunks of %s failed",
                                        ChunkID.chunkIDArrayToString(chunkIds));
                            }
                        }

                        rangeStart += batchChunkCount;

                        batches--;
                    }
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
            return -3;
        }

        System.out.print("Times per thread:");
        for (int i = 0; i < m_numThreads; i++) {
            System.out.printf("\nThread-%d: %f sec", i, time[i].getAccumulatedTimeAsUnit().getSecDouble());
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_numThreads; i++) {
            long t = time[i].getAccumulatedTime();
            if (t > totalTime) {
                totalTime = t;
            }
        }

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        System.out.printf("Throughput: %f chunks/sec\n",
                1000.0 * 1000.0 * 1000.0 / ((double) totalTime / activeChunkCount));

        allChunkRanges = chunkService.cidStatus().getAllLocalChunkIDRanges();

        System.out.printf("Available chunk ranges after remove:\n%s\n", allChunkRanges);

        // the index chunk will always be there (> 1)
        if (allChunkRanges.size() > 1) {
            System.out.println("Remove all failed, not all chunks are removed");
            return -4;
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_chunkBatch);
        p_exporter.writeInt(m_pattern);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt(m_numThreads);
        m_chunkBatch = p_importer.readInt(m_chunkBatch);
        m_pattern = p_importer.readInt(m_pattern);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3;
    }
}
