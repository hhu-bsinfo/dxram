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

package de.hhu.bsinfo.dxram.run.beineke;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.utils.RandomUtils;

/**
 * Test case for the logging interface
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 28.07.2014
 */
public final class LogTest implements Runnable {

    // Attributes
    private static DXRAM ms_dxram;
    private static ChunkService ms_chunkService;

    private static int ms_numberOfThreads;
    private static int ms_minChunkSize;
    private static int ms_maxChunkSize;
    private static long ms_numberOfChunks;
    private static long ms_chunksPerThread;
    private static int ms_numberOfUpdates;
    private static int ms_updatesPerThread;
    private static int ms_numberOfDeletes;
    private static int ms_deletesPerThread;
    private short m_nodeID;
    private int m_id;

    // Constructors

    /**
     * Creates an instance of LogTest
     *
     * @param p_nodeID
     *     the NodeID
     * @param p_id
     *     the thread identifier
     */
    private LogTest(final short p_nodeID, final int p_id) {
        m_nodeID = p_nodeID;
        m_id = p_id;
    }

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        long timeStart;
        short[] nodes;
        Thread[] threads;

        if (p_arguments.length == 6) {
            ms_numberOfThreads = Integer.parseInt(p_arguments[0]);
            ms_numberOfChunks = Long.parseLong(p_arguments[1]);
            ms_chunksPerThread = ms_numberOfChunks / ms_numberOfThreads;
            ms_minChunkSize = Integer.parseInt(p_arguments[2]);
            ms_maxChunkSize = Integer.parseInt(p_arguments[3]);
            ms_numberOfUpdates = Integer.parseInt(p_arguments[4]);
            ms_updatesPerThread = ms_numberOfUpdates / ms_numberOfThreads;
            ms_numberOfDeletes = Integer.parseInt(p_arguments[5]);
            ms_deletesPerThread = ms_numberOfDeletes / ms_numberOfThreads;

            if (ms_chunksPerThread > Integer.MAX_VALUE) {
                System.out.println("Too many chunks per thread! Exiting.");
                System.exit(-1);
            }
        } else if (p_arguments.length > 6) {
            System.out.println("Too many program arguments! Exiting.");
            System.exit(-1);
        } else {
            System.out.println("Missing program arguments (#threads, #chunks, minimal chunk size, maximal chunk size, #updates, #deletes)! Exiting.");
            System.exit(-1);
        }

        threads = new Thread[ms_numberOfThreads];

        // Initialize DXRAM
        ms_dxram = new DXRAM();
        ms_dxram.initialize("config/dxram.conf");
        ms_chunkService = ms_dxram.getService(ChunkService.class);

        timeStart = System.currentTimeMillis();
        nodes = new short[ms_numberOfThreads];
        for (int i = 0; i < ms_numberOfThreads; i++) {
            nodes[i] = (short) (Math.random() * (65536 - 1 + 1) + 1);

            threads[i] = new Thread(new LogTest(nodes[i], i));
            threads[i].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException ignored) {
                System.out.println("Error while joining threads");
            }
        }
        System.out.println("All chunks logged in " + (System.currentTimeMillis() - timeStart) + "ms");

        /*-timeStart = System.currentTimeMillis();
        for (short i = 0; i < m_numberOfNodes; i++) {
            try {
                log.printMetadataOfAllEntries(nodes[i]);
                // log.readAllEntries(nodes[i]);
            } catch (final DXRAMException e) {
                System.out.println("Error: Could not read entries of node " + nodes[i]);
            }
        }
        System.out.println("All chunks read in " + (System.currentTimeMillis() - timeStart) + "ms");*/
    }

    @Override
    public void run() {
        long[] removes;
        DSByteBuffer[] chunks;
        DSByteBuffer[] updates;
        DSByteBuffer[] fillChunks;
        ArrayList<Long> chunkIDList;
        ArrayList<DSByteBuffer> chunkList;

        System.out
            .println("I am " + m_id + ", writing " + ms_chunksPerThread + " chunks between " + ms_minChunkSize + " Bytes and " + ms_maxChunkSize + " Bytes");

        /*
         * Preparation
         */
        // Create array of Chunks
        chunks = new DSByteBuffer[(int) ms_chunksPerThread];
        for (int i = 0; i < ms_chunksPerThread; i++) {
            chunks[i] = new DSByteBuffer(RandomUtils.getRandomValue(ms_minChunkSize, ms_maxChunkSize));
            chunks[i].getData().put(("This is a test! (" + m_nodeID + ')').getBytes());
        }
        ms_chunkService.create(chunks);

        // Create list for updates (chunks)
        chunkList = new ArrayList<DSByteBuffer>();
        chunkList.addAll(Arrays.asList(chunks).subList(0, (int) ms_chunksPerThread));
        Collections.shuffle(chunkList);
        updates = chunkList.subList(0, ms_updatesPerThread).toArray(new DSByteBuffer[ms_updatesPerThread]);

        // Create list for deletes (chunkIDs)
        chunkIDList = new ArrayList<Long>();
        for (int i = 0; i < ms_chunksPerThread; i++) {
            chunkIDList.add(chunks[i].getID());
        }
        Collections.shuffle(chunkIDList);
        removes = chunkIDList.subList(0, ms_deletesPerThread).stream().mapToLong(l -> l).toArray();

        // Create fill chunks (to clear secondary log buffer)
        fillChunks = new DSByteBuffer[10];
        for (int i = 0; i < 10; i++) {
            fillChunks[i] = new DSByteBuffer(1048576);
        }

        /*
         * Execution
         */
        // Put
        System.out.print("Starting replication...");
        ms_chunkService.put(chunks);
        System.out.println("done\n");

        // Updates
        System.out.print("Starting updates...");
        ms_chunkService.put(updates);
        System.out.println("done\n");

        // Delete
        System.out.print("Starting deletion...");
        ms_chunkService.remove(removes);
        System.out.println("done\n");

        // Put dummies
        System.out.print("Starting fill replication...");
        ms_chunkService.put(fillChunks);
        System.out.println("done");
    }
}
