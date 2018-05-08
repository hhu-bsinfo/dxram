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
import java.util.Arrays;
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

    private static String ms_workload;
    private static int ms_updates;
    private static int ms_batchSize = 10;
    private static int ms_size = 64;
    private static int ms_backupRangeSize = 256 * 1024 * 1024;
    private static String ms_accessMode = "raf";
    private static boolean ms_timestampsEnabled = false;
    private static int ms_logSegmentSize = 8;
    private static int ms_primaryBufferSize = 32;
    private static int ms_secondaryLogBufferSize = 128;
    private static int ms_utilizationReorgActivation = 60;
    private static int ms_utilizationReorgPrompt = 75;
    private static int ms_coldDataThresholdSec = 90;

    private static int ms_chunkCount;
    private static volatile long ms_timeStartLoading;
    private static volatile long ms_timeStartUpdating;
    private static long ms_chunksLogged = 0;
    private static long ms_chunksUpdated = 0;
    private static boolean ms_isLoading = true;

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

        long[] rangeMapping = null;
        Load loadThread = new Load();
        loadThread.setName(String.valueOf(0));

        ProgressThread progressThread = new ProgressThread(1000);

        progressThread.start();

        LOGGER.info("Starting load phase...");
        ms_timeStartLoading = System.nanoTime();

        loadThread.start();

        try {
            loadThread.join();
            rangeMapping = loadThread.getRangeMapping();
        } catch (InterruptedException ignore) {
            System.out.println("Interrupt. Aborting.");
            System.exit(-1);
        }

        long timeEndLoading = System.nanoTime();

        LOGGER.info("Load phase finished.");

        if (!"none".equals(ms_workload)) {
            LOGGER.info("Waiting...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            LOGGER.info("Starting workload...");
            Workload thread = null;
            if ("sequential".equals(ms_workload)) {
                thread = new Sequential(rangeMapping);
            } else if ("random".equals(ms_workload)) {
                thread = new Random(rangeMapping);
            } else if ("zipf".equals(ms_workload)) {
                thread = new Zipf(rangeMapping);
            } else if ("hotncold".equals(ms_workload)) {
                thread = new HotAndCold(rangeMapping);
            }

            assert thread != null;
            thread.setName(String.valueOf(0));

            thread.start();

            try {
                thread.join();
            } catch (InterruptedException ignore) {
            }

            LOGGER.info("Workload finished.");

            /*while (ms_chunksLogged < ms_chunkCount) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }*/
        }

        long timeEndUpdating = System.nanoTime();

        progressThread.shutdown();

        printResults(timeEndLoading - ms_timeStartLoading, timeEndUpdating - ms_timeStartUpdating);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
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
        if (p_arguments.length != 6 && p_arguments.length != 13) {
            System.out.println("To execute benchmark:");
            System.out.println("Normal:");
            System.out.println("Args: " + " <access mode (raf, dir or raw)> <chunk count> <chunk size> <batch size> " +
                    "<workload (none, sequential, random, zipf or hotncold)> <number of updates>");
            System.out.println("Extended:");
            System.out.println("Args: " + " <access mode (raf, dir or raw)> <chunk count> <chunk size> <batch size> " +
                    "<workload (none, sequential, random, zipf or hotncold)> <number of updates> " +
                    "<timestamps enabled> <segment size in MB> <primary buffer size in MB> " +
                    "<secondary log buffer size in KB> <utilization for reorganization activation (in percent)> " +
                    "<utilization to prompt reorganization (in percent)> <cold data threshold in sec>");
            System.exit(-1);
        }

        ms_accessMode = p_arguments[0];
        if (!"raf".equals(ms_accessMode) && !"dir".equals(ms_accessMode) && !"raw".equals(ms_accessMode)) {
            System.out.println("Invalid access mode! Using RandomAccessFile.");
            ms_accessMode = "raf";
        }
        ms_chunkCount = Integer.parseInt(p_arguments[1]);
        ms_size = Integer.parseInt(p_arguments[2]);
        ms_batchSize = Integer.parseInt(p_arguments[3]);

        ms_workload = p_arguments[4];
        if (!"none".equals(ms_workload) && !"sequential".equals(ms_workload) && !"random".equals(ms_workload) &&
                !"zipf".equals(ms_workload) && !"hotncold".equals(ms_workload)) {
            System.out.println("Invalid workload! Starting with \"none\".");
            ms_workload = "none";
        }
        ms_updates = Integer.parseInt(p_arguments[5]);

        if (p_arguments.length == 13) {
            ms_timestampsEnabled = Boolean.parseBoolean(p_arguments[6]);
            ms_logSegmentSize = Integer.parseInt(p_arguments[7]);
            ms_primaryBufferSize = Integer.parseInt(p_arguments[8]);
            ms_secondaryLogBufferSize = Integer.parseInt(p_arguments[9]);
            ms_utilizationReorgActivation = Integer.parseInt(p_arguments[10]);
            ms_utilizationReorgPrompt = Integer.parseInt(p_arguments[11]);
            ms_coldDataThresholdSec = Integer.parseInt(p_arguments[12]);
        }

        System.out.printf("Parameters: access_mode=%s chunk_count=%d chunk_size=%d batch_size=%d " +
                        "workload=%s updates=%d timestamps=%s segment_size=%d primary_buffer_size=%d " +
                        "secondary_log_buffer_size=%d reorg_activation_utilization=%d " +
                        "reorg_prompt_utilization=%d cold_data_threshold_sec=%d\n", ms_accessMode, ms_chunkCount, ms_size,
                ms_batchSize, ms_workload, ms_updates, ms_timestampsEnabled, ms_logSegmentSize, ms_primaryBufferSize,
                ms_secondaryLogBufferSize, ms_utilizationReorgActivation, ms_utilizationReorgPrompt,
                ms_coldDataThresholdSec);
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
        ms_conf =
                new LogComponentConfig(ms_accessMode, "/dev/raw/raw1", true, ms_timestampsEnabled, 4, ms_logSegmentSize,
                        256, ms_primaryBufferSize, ms_secondaryLogBufferSize, ms_utilizationReorgActivation,
                        ms_utilizationReorgPrompt, ms_coldDataThresholdSec);
        ms_log.initComponent(ms_conf, pathLogFiles, ms_backupRangeSize);
    }

    /**
     * Print results.
     *
     * @param p_timeDiffLoadingNs
     *         time difference between start and end of loading phase.
     * @param p_timeDiffUpdatingNs
     *         time difference between start and end of updating phase.
     */
    private static void printResults(final long p_timeDiffLoadingNs, final long p_timeDiffUpdatingNs) {
        System.out.printf("[RESULTS LOADING]\n" + "[CHUNK SIZE] %d\n" + "[BATCH SIZE] %d\n" + "[RUNTIME] %d ms\n" +
                        "[TIME PER CHUNK] %d ns\n" + "[THROUGHPUT] %f MB/s\n" + "[THROUGHPUT OVERHEAD] %f MB/s\n", ms_size,
                ms_batchSize, p_timeDiffLoadingNs / 1000 / 1000,
                ms_chunkCount != 0 ? p_timeDiffLoadingNs / ms_chunkCount : ms_chunkCount, ms_chunkCount != 0 ?
                        (double) ms_chunkCount * ms_size / 1024 / 1024 /
                                ((double) p_timeDiffLoadingNs / 1000 / 1000 / 1000) : 0, ms_chunkCount != 0 ?
                        (double) ms_chunkCount * (ms_size + ObjectSizeUtil.sizeofCompactedNumber(ms_size) +
                                AbstractSecLogEntryHeader
                                        .getApproxSecLogHeaderSize(false, ms_chunkCount / 2, ms_size)) / 1024 / 1024 /
                                ((double) p_timeDiffLoadingNs / 1000 / 1000 / 1000) : 0);

        System.out.printf("[RESULTS UPDATING]\n" + "[RUNTIME] %d ms\n" + "[TIME PER CHUNK] %d ns\n" +
                        "[THROUGHPUT] %f MB/s\n" + "[THROUGHPUT OVERHEAD] %f MB/s\n", p_timeDiffUpdatingNs / 1000 / 1000,
                ms_updates != 0 ? p_timeDiffUpdatingNs / ms_updates : ms_updates, ms_updates != 0 ?
                        (double) ms_updates * ms_size / 1024 / 1024 /
                                ((double) p_timeDiffUpdatingNs / 1000 / 1000 / 1000) : 0, ms_updates != 0 ?
                        (double) ms_updates * (ms_size + ObjectSizeUtil.sizeofCompactedNumber(ms_size) +
                                AbstractSecLogEntryHeader
                                        .getApproxSecLogHeaderSize(false, ms_chunkCount / 2, ms_size)) / 1024 / 1024 /
                                ((double) p_timeDiffUpdatingNs / 1000 / 1000 / 1000) : 0);
    }

    private static class Load extends Thread {

        private long[] m_rangeMapping;

        /**
         * Constructor
         */
        Load() {
            super();
        }

        long[] getRangeMapping() {
            return m_rangeMapping;
        }

        @Override
        public void run() {
            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);
            int chunksPerRange = ms_backupRangeSize /
                    (ms_size + AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(false, ms_chunkCount, ms_size));
            m_rangeMapping = new long[ms_chunkCount / chunksPerRange];

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            byte[] array = new byte[ms_size];
            Arrays.fill(array, (byte) 5);
            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(array);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            long chunkID = ((long) 2 << 48) + 1;
            short rangeID = (short) 0;
            ms_log.incomingInitBackupRange(rangeID, (short) 2);
            for (int i = 0; i < ms_chunkCount / ms_batchSize; i++) {
                // Create a new range if necessary
                if ((ms_chunksLogged + ms_batchSize) / chunksPerRange > rangeID) {
                    m_rangeMapping[rangeID] = ms_chunksLogged;
                    ms_log.incomingInitBackupRange(++rangeID, (short) 2);
                }

                // Update ChunkIDs
                for (int j = 0; j < ms_batchSize; j++) {
                    buffer.putLong(j * entrySize, chunkID++);
                }

                buffer.position(0);
                ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                ms_chunksLogged += ms_batchSize;
            }
        }
    }

    /**
     * The worker thread executing the workload.
     */
    private abstract static class Workload extends Thread {

        long[] m_rangeMapping;

        abstract void initializeDistribution();

        abstract long getNextChunkID();

        Workload(final long[] p_rangeMapping) {
            m_rangeMapping = p_rangeMapping;
        }
    }

    private static class Sequential extends Workload {

        private long m_chunkID;

        /**
         * Constructor
         */
        Sequential(final long[] p_rangeMapping) {
            super(p_rangeMapping);
        }

        @Override
        void initializeDistribution() {
            m_chunkID = ((long) 2 << 48) + 1;

            ms_timeStartUpdating = System.nanoTime();
            ms_isLoading = false;
        }

        @Override
        long getNextChunkID() {
            return m_chunkID++;
        }

        @Override
        public void run() {
            initializeDistribution();

            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            byte[] array = new byte[ms_size];
            Arrays.fill(array, (byte) 7);
            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(array);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            long chunksLogged;
            short rangeID;
            while (ms_chunksUpdated < ms_updates) {
                chunksLogged = 0;
                rangeID = (short) 0;
                for (int i = 0; i < ms_chunkCount / ms_batchSize && ms_chunksUpdated < ms_updates; i++) {
                    if (m_rangeMapping.length > rangeID && chunksLogged == m_rangeMapping[rangeID]) {
                        rangeID++;
                    }

                    // Update ChunkIDs
                    for (int j = 0; j < ms_batchSize; j++) {
                        buffer.putLong(j * entrySize, getNextChunkID());
                    }

                    buffer.position(0);
                    ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                    chunksLogged += ms_batchSize;
                    ms_chunksUpdated += ms_batchSize;
                }
            }
        }
    }

    private static class Random extends Workload {

        private java.util.Random m_rand;

        /**
         * Constructor
         */
        Random(final long[] p_rangeMapping) {
            super(p_rangeMapping);
        }

        @Override
        void initializeDistribution() {
            long time = System.nanoTime();
            LOGGER.info("Initializing random distribution (seed: %d)", time);
            LOGGER.info("\tEstimated memory consumption: 0 MB");

            m_rand = new java.util.Random(time);

            LOGGER.info("Finished initializing distribution");

            ms_timeStartUpdating = System.nanoTime();
            ms_isLoading = false;
        }

        @Override
        long getNextChunkID() {
            long next = (m_rand.nextLong() & 0xFFFFFFFFFFFFL) % ms_chunkCount;
            return ((long) 2 << 48) + next + 1;
        }

        @Override
        public void run() {
            initializeDistribution();

            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            byte[] array = new byte[ms_size];
            Arrays.fill(array, (byte) 7);
            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(array);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            short rangeID;
            for (int i = 0; i < ms_updates / ms_batchSize; i++) {
                long chunkID = getNextChunkID();
                long localID = chunkID & 0x0000FFFFFFFFFFFFL;
                rangeID = 0;
                if (m_rangeMapping.length > 0) {
                    while (localID > m_rangeMapping[rangeID] && rangeID < m_rangeMapping.length - 1) {
                        rangeID++;
                    }

                    // All chunks must belong to the same backup range. Check barrier and move ChunkID if necessary
                    if (localID + ms_batchSize > m_rangeMapping[rangeID]) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + m_rangeMapping[rangeID] - ms_batchSize;
                    }
                } else {
                    if (localID + ms_batchSize > ms_chunkCount) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + ms_chunkCount - ms_batchSize;
                    }
                }

                // Update ChunkIDs
                for (int j = 0; j < ms_batchSize; j++) {
                    buffer.putLong(j * entrySize, chunkID++);
                }

                buffer.position(0);
                ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                ms_chunksUpdated += ms_batchSize;
            }
        }
    }

    private static class Zipf extends Workload {

        private double m_skew = 1.0f;
        private java.util.Random m_rand;

        private double[] m_probs;
        private int[] m_permutation;

        /**
         * Constructor
         */
        Zipf(final long[] p_rangeMapping) {
            super(p_rangeMapping);
        }

        @Override
        void initializeDistribution() {
            LOGGER.info("Initializing zipf distribution (size: %d, skew: %f)", ms_chunkCount, m_skew);
            LOGGER.info("\tEstimated memory consumption: %d MB",
                    (long) ms_chunkCount * (Double.BYTES + Integer.BYTES) / 1024 / 1024);

            m_rand = new java.util.Random(System.nanoTime());
            m_probs = new double[ms_chunkCount];

            LOGGER.info("\tCreating probability map");
            if (Double.compare(m_skew, 1.0f) == 0) {
                double div = 0;
                for (int i = 1; i <= ms_chunkCount; i++) {
                    div += 1 / (double) i;
                }

                double sum = 0;
                for (int i = 1; i <= ms_chunkCount; i++) {
                    double p = 1.0f / (double) i / div;
                    sum += p;
                    m_probs[i - 1] = sum;

                    if (i % (ms_chunkCount / 10) == 0) {
                        LOGGER.info("\t\t Progress: %d%%", (int) ((double) i / ms_chunkCount * 100));
                    }
                }
            } else {
                double div = 0;
                for (int i = 1; i <= ms_chunkCount; i++) {
                    div += 1 / Math.pow(i, m_skew);
                }

                double sum = 0;
                for (int i = 1; i <= ms_chunkCount; i++) {
                    double p = 1.0f / Math.pow(i, m_skew) / div;
                    sum += p;
                    m_probs[i - 1] = sum;

                    if (i % (ms_chunkCount / 10) == 0) {
                        LOGGER.info("\t\t Progress: %d%%", (int) ((double) i / ms_chunkCount * 100));
                    }
                }
            }

            LOGGER.info("\tCreating permutation map");
            m_permutation = new int[ms_chunkCount];
            for (int i = 0; i < ms_chunkCount; i++) {
                m_permutation[i] = i;
            }
            for (int i = 0; i < ms_chunkCount; i++) {
                int rand = i + m_rand.nextInt(ms_chunkCount - i);

                int element = m_permutation[rand];
                m_permutation[rand] = m_permutation[i];
                m_permutation[i] = element;

                if (i % (ms_chunkCount / 10) == 0 && i != 0) {
                    LOGGER.info("\t\t Progress: %d%%", (int) Math.ceil((double) i / ms_chunkCount * 100));
                }
            }

            LOGGER.info("Finished initializing distribution");

            ms_timeStartUpdating = System.nanoTime();
            ms_isLoading = false;
        }

        @Override
        long getNextChunkID() {
            float value = m_rand.nextFloat();
            int index = Arrays.binarySearch(m_probs, value);
            if (index < 0) {
                index = index * -1 + 1;
            }

            return ((long) 2 << 48) + m_permutation[index];
        }

        @Override
        public void run() {
            initializeDistribution();

            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            byte[] array = new byte[ms_size];
            Arrays.fill(array, (byte) 7);
            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(array);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            short rangeID;
            for (int i = 0; i < ms_updates / ms_batchSize; i++) {
                long chunkID = getNextChunkID();
                long localID = chunkID & 0x0000FFFFFFFFFFFFL;
                rangeID = 0;
                if (m_rangeMapping.length > 0) {
                    while (localID > m_rangeMapping[rangeID] && rangeID < m_rangeMapping.length - 1) {
                        rangeID++;
                    }

                    // All chunks must belong to the same backup range -> check barrier and move ChunkID if necessary
                    if (localID + ms_batchSize > m_rangeMapping[rangeID]) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + m_rangeMapping[rangeID] - ms_batchSize;
                    }
                } else {
                    if (localID + ms_batchSize > ms_chunkCount) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + ms_chunkCount - ms_batchSize;
                    }
                }

                // Update ChunkIDs
                for (int j = 0; j < ms_batchSize; j++) {
                    buffer.putLong(j * entrySize, chunkID++);
                }

                buffer.position(0);
                ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                ms_chunksUpdated += ms_batchSize;
            }
        }
    }

    private static class HotAndCold extends Workload {

        private static final float HOT_FRACTION = 0.1f;
        private static final float HOT_PROBABILITY = 0.9f;

        private int[] m_hot;
        private int[] m_cold;

        private java.util.Random m_rand;

        /**
         * Constructor
         */
        HotAndCold(final long[] p_rangeMapping) {
            super(p_rangeMapping);
        }

        @Override
        void initializeDistribution() {
            LOGGER.info("Initializing hot-and-cold distribution (hot fraction: %f, hot probability: %f)", HOT_FRACTION,
                    HOT_PROBABILITY);
            LOGGER.info("\tEstimated memory consumption: %d MB", ms_chunkCount * Integer.BYTES / 1024 / 1024);

            m_rand = new java.util.Random(System.nanoTime());

            int numberOfHotObjects = (int) (ms_chunkCount * HOT_FRACTION);
            int numberOfColdObjects = ms_chunkCount - numberOfHotObjects;
            int indexHot = 0;
            int indexCold = 0;

            m_hot = new int[numberOfHotObjects];
            m_cold = new int[numberOfColdObjects];

            float prob;
            for (int i = 0; i < ms_chunkCount; i++) {
                prob = m_rand.nextFloat();
                if (prob < HOT_FRACTION && indexHot != numberOfHotObjects || indexCold == numberOfColdObjects) {
                    m_hot[indexHot++] = i;
                } else {
                    m_cold[indexCold++] = i;
                }

                if (i % (ms_chunkCount / 10) == 0 && i != 0) {
                    LOGGER.info("\t\t Progress: %d%%", (int) Math.ceil((double) i / ms_chunkCount * 100));
                }
            }

            LOGGER.info("Finished initializing distribution");

            ms_timeStartUpdating = System.nanoTime();
            ms_isLoading = false;
        }

        @Override
        long getNextChunkID() {
            int index;
            float prob = m_rand.nextFloat();
            if (prob < HOT_PROBABILITY) {
                index = m_rand.nextInt((int) (ms_chunkCount * HOT_FRACTION));
                return ((long) 2 << 48) + m_hot[index] + 1;
            } else {
                index = m_rand.nextInt(ms_chunkCount - (int) (ms_chunkCount * HOT_FRACTION));
                return ((long) 2 << 48) + m_cold[index] + 1;
            }
        }

        @Override
        public void run() {
            initializeDistribution();

            int entrySize = ms_size + Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(ms_size);

            ByteBuffer buffer = ByteBuffer.allocateDirect(ms_batchSize * (ms_size + 8 + 4));
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            byte[] array = new byte[ms_size];
            Arrays.fill(array, (byte) 7);
            ByteBufferImExporter imExporter = new ByteBufferImExporter(buffer);
            for (int i = 0; i < ms_batchSize; i++) {
                DataStructure ds = new DSByteArray(array);
                imExporter.writeLong(ds.getID());
                imExporter.writeCompactNumber(ms_size);
                imExporter.exportObject(ds);
            }

            short rangeID;
            for (int i = 0; i < ms_updates / ms_batchSize; i++) {
                long chunkID = getNextChunkID();
                long localID = chunkID & 0x0000FFFFFFFFFFFFL;
                rangeID = 0;
                if (m_rangeMapping.length > 0) {
                    while (localID > m_rangeMapping[rangeID] && rangeID < m_rangeMapping.length - 1) {
                        rangeID++;
                    }

                    // All chunks must belong to the same backup range -> check barrier and move ChunkID if necessary
                    if (localID + ms_batchSize > m_rangeMapping[rangeID]) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + m_rangeMapping[rangeID] - ms_batchSize;
                    }
                } else {
                    if (localID + ms_batchSize > ms_chunkCount) {
                        chunkID = (chunkID & 0xFFFF000000000000L) + ms_chunkCount - ms_batchSize;
                    }
                }

                // Update ChunkIDs
                for (int j = 0; j < ms_batchSize; j++) {
                    buffer.putLong(j * entrySize, chunkID++);
                }

                buffer.position(0);
                ms_log.incomingLogChunks(rangeID, ms_batchSize, buffer, (short) 2);
                ms_chunksUpdated += ms_batchSize;
            }
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

                long chunksLogged;
                long timeDiff;
                long allChunks;
                if (ms_isLoading) {
                    chunksLogged = ms_chunksLogged;
                    allChunks = ms_chunkCount;
                    timeDiff = System.nanoTime() - ms_timeStartLoading;
                } else {
                    chunksLogged = ms_chunksUpdated;
                    allChunks = ms_updates;
                    timeDiff = System.nanoTime() - ms_timeStartUpdating;
                }
                System.out.printf("[PROGRESS] %d sec: Logged %d%% (%d), Throughput %f, Throughput(Overhead) %f\n",
                        timeDiff / 1000 / 1000 / 1000,
                        allChunks != 0 ? (int) ((float) chunksLogged / allChunks * 100) : 0, chunksLogged,
                        (double) chunksLogged * ms_size / 1024 / 1024 / ((double) timeDiff / 1000 / 1000 / 1000),
                        (double) chunksLogged * (ms_size + ObjectSizeUtil.sizeofCompactedNumber(ms_size) +
                                AbstractSecLogEntryHeader
                                        .getApproxSecLogHeaderSize(false, ms_chunkCount / 2, ms_size)) / 1024 / 1024 /
                                ((double) timeDiff / 1000 / 1000 / 1000));
            }
        }
    }
}
