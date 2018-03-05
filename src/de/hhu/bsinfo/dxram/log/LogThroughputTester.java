/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;

/**
 * Class for testing the logging and reorganization without starting DXRAM. Chunks are NOT send over network.
 * Example:
 * java -Dlog4j.configurationFile=config/log4j.xml -cp lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:
 * dxram.jar
 * de.hhu.bsinfo.dxram.log.LogThroughputTester
 * 10000000 10 1024
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 28.01.2018
 */
public final class LogThroughputTester {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LogThroughputTester.class.getSimpleName());

    private static LogComponent ms_log;
    private static LogComponentConfig ms_conf;

    private static int ms_batchSize = 10;
    private static int ms_size = 64;
    private static int ms_backupRangeSize = 256 * 1024 * 1024;
    private static String ms_accessMode = "raf";

    private static long ms_chunkCount;
    private static volatile long ms_timeStart;
    private static long ms_chunksLogged = 0;

    /**
     * Hidden constructor.
     */
    private LogThroughputTester() {

    }

    /**
     * Main method for initializing and starting the benchmark.
     *
     * @param p_arguments
     *         the program arguments.
     */
    public static void main(final String[] p_arguments) {
        Locale.setDefault(new Locale("en", "US"));

        processArgs(p_arguments);

        setup();

        Workload thread = new Workload();
        thread.setName(String.valueOf(0));

        LOGGER.info("Starting workload...");
        ProgressThread progressThread = new ProgressThread(1000);

        progressThread.start();

        ms_timeStart = System.nanoTime();

        thread.start();

        try

        {
            thread.join();
        } catch (InterruptedException ignore)

        {
        }

        LOGGER.info("Workload finished.");

        while (ms_chunksLogged < ms_chunkCount)

        {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }

        long timeEnd = System.nanoTime();

        progressThread.shutdown();

        printResults(timeEnd - ms_timeStart);

        try

        {
            Thread.sleep(3000);
        } catch (InterruptedException e)

        {
            e.printStackTrace();
        }

        StatisticsManager.get().stopPeriodicPrinting();
        StatisticsManager.get().printStatistics(System.out);

        System.exit(0);
    }

    /**
     * Process the program arguments.
     *
     * @param p_arguments
     *         the program arguments.
     */
    private static void processArgs(final String[] p_arguments) {
        if (p_arguments.length != 4) {
            System.out.println("To execute benchmark:");
            System.out.println("Args: <access mode (raf, dir or raw)> <chunk count> <batch size> <chunk size>");
            System.exit(-1);
        }

        ms_accessMode = p_arguments[0];
        if (!"raf".equals(ms_accessMode) && !"dir".equals(ms_accessMode) && !"raw".equals(ms_accessMode)) {
            System.out.println("Invalid access mode! Using RandomAccessFile.");
            ms_accessMode = "raf";
        }
        ms_chunkCount = Long.parseLong(p_arguments[1]);
        ms_batchSize = Integer.parseInt(p_arguments[2]);
        ms_size = Integer.parseInt(p_arguments[3]);

        System.out.printf("Parameters: log count %d, batch size %d, size %d\n", ms_chunkCount, ms_batchSize, ms_size);
    }

    /**
     * Setup files and classes for the benchmark.
     */
    private static void setup() {
        String pathLogFiles = "/media/ssd/dxram_log/";
        File[] files = new File(pathLogFiles).listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    if (!file.delete()) {
                        System.out.println("Could not delete old log file (" + file.getName() + ").");
                    }
                }
            }
        }

        ms_log = new LogComponent();
        ms_conf = new LogComponentConfig(ms_accessMode, "/dev/raw/raw1", true, false, 4, 8, 256, 256, 128, 70, 9000);
        ms_log.initComponent(ms_conf, pathLogFiles, ms_backupRangeSize);
    }

    /**
     * Print results.
     *
     * @param p_timeDiffNs
     *         time difference between start and end.
     */
    private static void printResults(final long p_timeDiffNs) {
        System.out.printf("[RESULTS]\n" + "[CHUNK SIZE] %d\n" + "[BATCH SIZE] %d\n" + "[RUNTIME] %d ms\n" +
                        "[TIME PER CHUNK] %d ns\n" +
                        "[THROUGHPUT] %f MB/s\n" + "[THROUGHPUT OVERHEAD] %f MB/s\n", ms_size, ms_batchSize,
                p_timeDiffNs / 1000 / 1000,
                ms_chunkCount != 0 ? p_timeDiffNs / ms_chunkCount : ms_chunkCount,
                ms_chunkCount != 0 ?
                        (double) ms_chunkCount * ms_size / 1024 / 1024 / ((double) p_timeDiffNs / 1000 / 1000 / 1000) :
                        0, ms_chunkCount != 0 ?
                        (double) ms_chunkCount * (ms_size + ObjectSizeUtil.sizeofCompactedNumber(ms_size) + 10) / 1024 /
                                1024 /
                                ((double) p_timeDiffNs / 1000 / 1000 / 1000) : 0);
    }

    /**
     * The worker thread executing the workload.
     */
    private static class Workload extends Thread {

        /**
         * Constructor
         */
        Workload() {
            super();
        }

        @Override
        public void run() {
            long chunkID = ((long) 2 << 48) + 1;
            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);
            int chunksPerRange = ms_backupRangeSize / (ms_size + AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(
                    false, ms_chunkCount, ms_size));

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(chunkID, ms_size);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            short rangeID = (short) 0;
            ms_log.incomingInitBackupRange(rangeID, (short) 2);
            for (int i = 0; i < ms_chunkCount / ms_batchSize; i++) {
                // Create a new range if necessary
                if ((ms_chunksLogged + ms_batchSize) / chunksPerRange > rangeID) {
                    ms_log.incomingInitBackupRange(++rangeID, (short) 2);
                }

                // Update ChunkIDs
                for (int j = 0; j < ms_batchSize; j++) {
                    buffer.putLong(j * entrySize, ++chunkID);
                }

                buffer.position(0);
                ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                ms_chunksLogged += ms_batchSize;
            }
        }

        static void bufferTest() {
            System.out.println("ByteBuffer performance test:");
            int length = 8 * 1024 * 1024;
            int iterations = 1000;
            long start;
            long timeWrite = 0;
            long timeRead = 0;
            long l2 = 0;

            ByteBuffer[] buffersDirect = new ByteBuffer[iterations];
            for (int j = 0; j < iterations; j++) {
                buffersDirect[j] = ByteBuffer.allocateDirect(length);
            }
            for (int j = 0; j < iterations; j++) {
                start = System.nanoTime();
                buffersDirect[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    buffersDirect[j].putLong(37);
                    buffersDirect[j].putShort((short) 12);
                    buffersDirect[j].put((byte) 7);
                    buffersDirect[j].put((byte) 3);
                    buffersDirect[j].put((byte) 16);
                }
                timeWrite += System.nanoTime() - start;

                start = System.nanoTime();
                buffersDirect[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    long l = buffersDirect[j].getLong();
                    short sh = buffersDirect[j].getShort();
                    byte b1 = buffersDirect[j].get();
                    byte b2 = buffersDirect[j].get();
                    byte b3 = buffersDirect[j].get();
                    l2 = l + sh + b1 + b2 + b3;
                }
                timeRead += System.nanoTime() - start;
            }
            System.out.println(
                    "DirectByteBuffer: write: " + timeWrite / 1000 + ", read: " + timeRead / 1000 + ", " + l2);

            timeWrite = 0;
            timeRead = 0;
            buffersDirect = new ByteBuffer[iterations];
            for (int j = 0; j < iterations; j++) {
                buffersDirect[j] = ByteBuffer.allocateDirect(length);
                buffersDirect[j].order(ByteOrder.LITTLE_ENDIAN);
            }
            for (int j = 0; j < iterations; j++) {
                start = System.nanoTime();
                buffersDirect[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    buffersDirect[j].putLong(37);
                    buffersDirect[j].putShort((short) 12);
                    buffersDirect[j].put((byte) 7);
                    buffersDirect[j].put((byte) 3);
                    buffersDirect[j].put((byte) 16);
                }
                timeWrite += System.nanoTime() - start;

                start = System.nanoTime();
                buffersDirect[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    long l = buffersDirect[j].getLong();
                    short sh = buffersDirect[j].getShort();
                    byte b1 = buffersDirect[j].get();
                    byte b2 = buffersDirect[j].get();
                    byte b3 = buffersDirect[j].get();
                    l2 = l + sh + b1 + b2 + b3;
                }
                timeRead += System.nanoTime() - start;
            }
            System.out.println(
                    "DirectByteBuffer littleEndian: write: " + timeWrite / 1000 + ", read: " + timeRead / 1000 + ", " +
                            l2);

            timeWrite = 0;
            timeRead = 0;
            ByteBuffer[] buffers = new ByteBuffer[iterations];
            for (int j = 0; j < iterations; j++) {
                buffers[j] = ByteBuffer.allocate(length);
            }
            for (int j = 0; j < iterations; j++) {
                start = System.nanoTime();
                buffers[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    buffers[j].putLong(37);
                    buffers[j].putShort((short) 12);
                    buffers[j].put((byte) 7);
                    buffers[j].put((byte) 3);
                    buffers[j].put((byte) 16);
                }
                timeWrite += System.nanoTime() - start;

                start = System.nanoTime();
                buffers[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    long l = buffers[j].getLong();
                    short sh = buffers[j].getShort();
                    byte b1 = buffers[j].get();
                    byte b2 = buffers[j].get();
                    byte b3 = buffers[j].get();
                    l2 = l + sh + b1 + b2 + b3;
                }
                timeRead += System.nanoTime() - start;
            }
            System.out.println("HeapByteBuffer: write: " + timeWrite / 1000 + ", read: " + timeRead / 1000 + ", " + l2);

            timeWrite = 0;
            timeRead = 0;
            buffers = new ByteBuffer[iterations];
            for (int j = 0; j < iterations; j++) {
                buffers[j] = ByteBuffer.allocate(length);
                buffers[j].order(ByteOrder.LITTLE_ENDIAN);
            }
            for (int j = 0; j < iterations; j++) {
                start = System.nanoTime();
                buffers[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    buffers[j].putLong(37);
                    buffers[j].putShort((short) 12);
                    buffers[j].put((byte) 7);
                    buffers[j].put((byte) 3);
                    buffers[j].put((byte) 16);
                }
                timeWrite += System.nanoTime() - start;

                start = System.nanoTime();
                buffers[j].position(0);
                for (int i = 0; i < length / 13; i++) {
                    long l = buffers[j].getLong();
                    short sh = buffers[j].getShort();
                    byte b1 = buffers[j].get();
                    byte b2 = buffers[j].get();
                    byte b3 = buffers[j].get();
                    l2 = l + sh + b1 + b2 + b3;
                }
                timeRead += System.nanoTime() - start;
            }
            System.out.println(
                    "HeapByteBuffer littleEndian: write: " + timeWrite / 1000 + ", read: " + timeRead / 1000 + ", " +
                            l2);

            timeWrite = 0;
            timeRead = 0;
            byte[][] arrays = new byte[iterations][];
            for (int j = 0; j < iterations; j++) {
                arrays[j] = new byte[length];
            }
            for (int j = 0; j < iterations; j++) {
                start = System.nanoTime();
                for (int i = 0; i < length / 13; i++) {
                    arrays[j][i * 13] = (byte) 37;
                    arrays[j][i * 13 + 1] = (byte) (37 << 8);
                    arrays[j][i * 13 + 2] = (byte) (37 << 16);
                    arrays[j][i * 13 + 3] = (byte) ((long) 37 << 24);
                    arrays[j][i * 13 + 4] = (byte) ((long) 37 << 32);
                    arrays[j][i * 13 + 5] = (byte) ((long) 37 << 40);
                    arrays[j][i * 13 + 6] = (byte) ((long) 37 << 48);
                    arrays[j][i * 13 + 7] = (byte) ((long) 37 << 56);

                    arrays[j][i * 13 + 8] = (byte) 12;
                    arrays[j][i * 13 + 9] = (byte) (12 << 8);

                    arrays[j][i * 13 + 10] = (byte) 7;
                    arrays[j][i * 13 + 11] = (byte) 3;
                    arrays[j][i * 13 + 12] = (byte) 16;
                }
                timeWrite += System.nanoTime() - start;

                start = System.nanoTime();
                for (int i = 0; i < length / 13; i++) {
                    long l = arrays[j][i * 13] + (arrays[j][i * 13 + 1] << 8) + (arrays[j][i * 13 + 2] << 16) +
                            ((long) arrays[j][i * 13 + 3] << 24) +
                            ((long) arrays[j][i * 13 + 4] << 32) + ((long) arrays[j][i * 13 + 5] << 40) +
                            ((long) arrays[j][i * 13 + 6] << 48) +
                            ((long) arrays[j][i * 13 + 7] << 56);

                    short sh = (short) (arrays[j][i * 13 + 8] + (arrays[j][i * 13 + 9] << 8));

                    byte b1 = arrays[j][i * 13 + 10];
                    byte b2 = arrays[j][i * 13 + 11];
                    byte b3 = arrays[j][i * 13 + 12];
                    l2 = l + sh + b1 + b2 + b3;
                }
                timeRead += System.nanoTime() - start;
            }
            System.out.println("Array: write: " + timeWrite / 1000 + ", read: " + timeRead / 1000 + ", " + l2);
        }
    }

    /**
     * A thread for periodically printing information.
     */
    private static class ProgressThread extends Thread {
        private volatile boolean m_run = true;
        private int m_intervalMs;

        /**
         * Constructor
         *
         * @param p_intervalMs
         *         the print interval.
         */
        ProgressThread(final int p_intervalMs) {
            m_intervalMs = p_intervalMs;
        }

        /**
         * Shut down.
         */
        public void shutdown() {
            m_run = false;

            try {
                join();
            } catch (InterruptedException ignored) {

            }
        }

        @Override
        public void run() {
            while (m_run) {
                try {
                    Thread.sleep(m_intervalMs);
                } catch (InterruptedException ignored) {

                }

                long chunksLogged = ms_chunksLogged;
                long timeDiff = System.nanoTime() - ms_timeStart;
                System.out.printf("[PROGRESS] %d sec: Logged %d%% (%d), Throughput %f, Throughput(Overhead) %f\n",
                        timeDiff / 1000 / 1000 / 1000,
                        ms_chunkCount != 0 ? (int) ((float) chunksLogged / ms_chunkCount * 100) : 0, chunksLogged,
                        (double) chunksLogged * ms_size / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000),
                        (double) chunksLogged *
                                (ms_size + ObjectSizeUtil.sizeofCompactedNumber(ms_size) +
                                        AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(false, ms_chunkCount / 2,
                                                ms_size)) / 1024 / 1024 /
                                ((double) timeDiff / 1000 / 1000 / 1000));
            }
        }
    }
}
