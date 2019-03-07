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

package de.hhu.bsinfo.dxram.nameservice.bench;

import java.util.Random;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.bench.ChunkTaskUtils;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Task to benchmark/test registering of nameservice entries
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 19.01.2017
 */
public class NameserviceRegisterTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NameserviceRegisterTask.class);

    private String m_allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGRHIJKLMNOPQRSTUVWXYZ0123456789-";

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_chunkCount = 1000;

    @Override
    public int execute(final TaskContext p_ctx) {

        NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        long[] chunkCountsPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_chunkCount, m_numThreads);

        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];

        System.out.printf("Registering %d chunks with %d thread(s)...\n", m_chunkCount, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {

                String[] randomNames = new String[(int) chunkCountsPerThread[threadIdx]];
                for (int k = 0; k < chunkCountsPerThread[threadIdx]; k++) {
                    randomNames[k] = getRandomValue();
                }

                timeStart[threadIdx] = System.nanoTime();

                for (int j = 0; j < chunkCountsPerThread[threadIdx]; j++) {
                    nameserviceService.register(1, randomNames[j]);
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
        System.out.printf("Throughput: %f chunks/sec\n",
                1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_chunkCount));

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_chunkCount);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numThreads = p_importer.readInt(m_numThreads);
        m_chunkCount = p_importer.readInt(m_chunkCount);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2;
    }

    private String getRandomValue() {
        Random random = new Random();
        int length = random.nextInt(5 + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(m_allowedChars.charAt(random.nextInt(m_allowedChars.length())));
        }
        return sb.toString();
    }
}
