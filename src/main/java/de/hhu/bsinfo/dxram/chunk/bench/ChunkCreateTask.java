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

import java.util.Arrays;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.Stopwatch;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Task to create chunks on a node using different patterns
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 25.01.2017
 */
public class ChunkCreateTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkCreateTask.class);

    private static final int PATTERN_LOCAL_ONLY = 0;
    private static final int PATTERN_REMOTE_ONLY_SUCCESSOR = 1;
    private static final int PATTERN_REMOTE_ONLY_RANDOM = 2;
    private static final int PATTERN_REMOTE_LOCAL_MIXED_RANDOM = 3;

    @Expose
    private int m_numThreads = 1;
    @Expose
    private long m_chunkCount = 1000;
    @Expose
    private int m_chunkBatch = 10;
    @Expose
    private StorageUnit m_chunkSizeBytesBegin = new StorageUnit(16, StorageUnit.BYTE);
    @Expose
    private StorageUnit m_chunkSizeBytesEnd = new StorageUnit(512, StorageUnit.BYTE);
    @Expose
    private int m_pattern = PATTERN_LOCAL_ONLY;

    public ChunkCreateTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {
        boolean remote = m_pattern > PATTERN_LOCAL_ONLY;

        if (remote && p_ctx.getCtxData().getSlaveNodeIds().length < 2) {
            System.out.println("Not enough slaves (min 2) to execute this task");
            return -1;
        }

        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);

        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_chunkCount, m_numThreads);
        Thread[] threads = new Thread[m_numThreads];
        Stopwatch[] time = new Stopwatch[m_numThreads];
        for (int i = 0; i < time.length; i++) {
            time[i] = new Stopwatch();
        }

        System.out.printf("Creating (pattern %d) %d chunks in batches of %d chunk(s) of random sizes between %s " +
                        "and %s with %d thread(s)...\n", m_pattern, m_chunkCount, m_chunkBatch, m_chunkSizeBytesBegin,
                m_chunkSizeBytesEnd, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {
                int[] sizes = new int[m_chunkBatch];
                long batches = chunkCountsPerThread[threadIdx] / m_chunkBatch;
                long lastBatchRemainder = chunkCountsPerThread[threadIdx] % m_chunkBatch;

                switch (m_pattern) {
                    case PATTERN_LOCAL_ONLY: {
                        for (int j = 0; j < batches; j++) {
                            for (int k = 0; k < m_chunkBatch; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkLocalService.createLocal().createSizes(chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s failed", Arrays.toString(sizes));
                            }
                        }

                        if (lastBatchRemainder > 0) {
                            sizes = new int[(int) lastBatchRemainder];
                            for (int k = 0; k < sizes.length; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkLocalService.createLocal().createSizes(chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s failed", Arrays.toString(sizes));
                            }
                        }

                        break;
                    }

                    case PATTERN_REMOTE_ONLY_SUCCESSOR: {
                        short destNodeId = ChunkTaskUtils.getSuccessorSlaveNodeId(p_ctx.getCtxData().getSlaveNodeIds(),
                                p_ctx.getCtxData().getSlaveId());

                        for (int j = 0; j < batches; j++) {
                            for (int k = 0; k < m_chunkBatch; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                        Arrays.toString(sizes), destNodeId);
                            }
                        }

                        if (lastBatchRemainder > 0) {
                            sizes = new int[(int) lastBatchRemainder];
                            for (int k = 0; k < sizes.length; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                        Arrays.toString(sizes), destNodeId);
                            }
                        }

                        break;
                    }

                    case PATTERN_REMOTE_ONLY_RANDOM: {
                        short ownNodeId = p_ctx.getCtxData().getSlaveNodeIds()[p_ctx.getCtxData().getSlaveId()];

                        for (int j = 0; j < batches; j++) {
                            short destNodeId = ChunkTaskUtils.getRandomNodeIdExceptOwn(
                                    p_ctx.getCtxData().getSlaveNodeIds(), ownNodeId);

                            for (int k = 0; k < m_chunkBatch; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                        Arrays.toString(sizes), destNodeId);
                            }
                        }

                        if (lastBatchRemainder > 0) {
                            short destNodeId = ChunkTaskUtils.getRandomNodeIdExceptOwn(
                                    p_ctx.getCtxData().getSlaveNodeIds(), ownNodeId);
                            sizes = new int[(int) lastBatchRemainder];

                            for (int k = 0; k < sizes.length; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                        Arrays.toString(sizes), destNodeId);
                            }
                        }

                        break;
                    }

                    case PATTERN_REMOTE_LOCAL_MIXED_RANDOM: {
                        short ownNodeId = p_ctx.getCtxData().getSlaveNodeIds()[p_ctx.getCtxData().getSlaveId()];

                        for (int j = 0; j < batches; j++) {
                            short destNodeId = ChunkTaskUtils.getRandomNodeId(p_ctx.getCtxData().getSlaveNodeIds());

                            for (int k = 0; k < m_chunkBatch; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            if (destNodeId == ownNodeId) {
                                time[threadIdx].start();
                                long[] chunkIDs = new long[sizes.length];
                                chunkLocalService.createLocal().createSizes(chunkIDs, sizes);
                                time[threadIdx].stopAndAccumulate();

                                if (chunkIDs == null) {
                                    LOGGER.error("Creating chunks for sizes %s failed", Arrays.toString(sizes));
                                }
                            } else {
                                time[threadIdx].start();
                                long[] chunkIDs = new long[sizes.length];
                                chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                                time[threadIdx].stopAndAccumulate();

                                if (chunkIDs == null) {
                                    LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                            Arrays.toString(sizes), destNodeId);
                                }
                            }
                        }

                        if (lastBatchRemainder > 0) {
                            short destNodeId = ChunkTaskUtils.getRandomNodeIdExceptOwn(
                                    p_ctx.getCtxData().getSlaveNodeIds(), ownNodeId);
                            sizes = new int[(int) lastBatchRemainder];

                            for (int k = 0; k < sizes.length; k++) {
                                sizes[k] = RandomUtils.getRandomSize(m_chunkSizeBytesBegin, m_chunkSizeBytesEnd);
                            }

                            time[threadIdx].start();
                            long[] chunkIDs = new long[sizes.length];
                            chunkService.create().createSizes(destNodeId, chunkIDs, sizes);
                            time[threadIdx].stopAndAccumulate();

                            if (chunkIDs == null) {
                                LOGGER.error("Creating chunks for sizes %s on remote 0x%X failed",
                                        Arrays.toString(sizes), destNodeId);
                            }
                        }

                        break;
                    }

                    default:
                        System.out.printf("Unsupported pattern %d", m_pattern);
                        break;
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
            return -1;
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
                1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_chunkCount));

        ChunkIDRanges allChunkRanges = chunkService.cidStatus().getAllLocalChunkIDRanges();

        System.out.printf("Available chunk ranges after create:\n%s\n", allChunkRanges);

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeLong(m_chunkCount);
        p_exporter.writeInt(m_chunkBatch);
        p_exporter.exportObject(m_chunkSizeBytesBegin);
        p_exporter.exportObject(m_chunkSizeBytesEnd);
        p_exporter.writeInt(m_pattern);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt(m_numThreads);
        m_chunkCount = p_importer.readLong(m_chunkCount);
        m_chunkBatch = p_importer.readInt(m_chunkBatch);
        m_chunkSizeBytesBegin = new StorageUnit();
        p_importer.importObject(m_chunkSizeBytesBegin);
        m_chunkSizeBytesEnd = new StorageUnit();
        p_importer.importObject(m_chunkSizeBytesEnd);
        m_pattern = p_importer.readInt(m_pattern);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES + Integer.BYTES + m_chunkSizeBytesBegin.sizeofObject() +
                m_chunkSizeBytesEnd.sizeofObject() + Integer.BYTES;
    }
}
