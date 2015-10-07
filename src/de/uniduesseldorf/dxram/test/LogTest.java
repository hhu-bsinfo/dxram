
package de.uniduesseldorf.dxram.test;

import java.util.ArrayList;
import java.util.Collections;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.utils.Tools;

/**
 * Test case for the logging interface
 * @author Kevin Beineke
 *         28.07.2014
 */
public final class LogTest implements Runnable {

	// Constants
	private static final boolean TEST_LOCALLY = false;

	// Attributes
	private static int m_numberOfThreads;
	private static int m_minChunkSize;
	private static int m_maxChunkSize;
	private static int m_updates;
	private static int m_deletes;
	private static long m_numberOfChunks;
	private static long m_chunksPerThread;
	private short m_nodeID;
	private int m_id;

	// Constructors
	/**
	 * Creates an instance of LogTest
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_log
	 *            the log interface
	 * @param p_id
	 *            the thread identifier
	 */
	private LogTest(final short p_nodeID, final LogInterface p_log, final int p_id) {
		m_nodeID = p_nodeID;
		m_id = p_id;
	}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		long timeStart;
		short[] nodes;
		Thread[] threads = null;
		LogInterface log = null;

		if (p_arguments.length == 6) {
			m_numberOfThreads = Integer.parseInt(p_arguments[0]);
			m_numberOfChunks = Long.parseLong(p_arguments[1]);
			m_chunksPerThread = m_numberOfChunks / m_numberOfThreads;
			m_minChunkSize = Integer.parseInt(p_arguments[2]);
			m_maxChunkSize = Integer.parseInt(p_arguments[3]);
			m_updates = Integer.parseInt(p_arguments[4]);
			m_deletes = Integer.parseInt(p_arguments[5]);

			if (m_chunksPerThread > Integer.MAX_VALUE) {
				System.out.println("Too many chunks per thread! Exiting.");
				System.exit(-1);
			}
		} else if (p_arguments.length > 6) {
			System.out.println("Too many program arguments! Exiting.");
			System.exit(-1);
		} else {
			System.out.println("Missing program arguments (#threads, number of chunks, " + "minimal chunk size, maximal chunk size)! Exiting.");
			System.exit(-1);
		}

		threads = new Thread[m_numberOfThreads];
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			log = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			System.out.println("Error during initialization of DXRAM");
		}

		timeStart = System.currentTimeMillis();
		nodes = new short[m_numberOfThreads];
		for (int i = 0; i < m_numberOfThreads; i++) {
			nodes[i] = (short) (Math.random() * (65536 - 1 + 1) + 1);

			threads[i] = new Thread(new LogTest(nodes[i], log, i));
			threads[i].start();
		}
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
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
		int[] sizes;
		long[] removes;
		Chunk[] chunks;
		Chunk[] updates;
		Chunk[] fillChunks;
		ArrayList<Long> chunkIDList;
		ArrayList<Chunk> chunkList;
		LogInterface log = null;

		System.out.println("I am " + m_id + ", writing " + m_chunksPerThread + " chunks between " + m_minChunkSize + " Bytes and " + m_maxChunkSize + " Bytes");

		try {
			/*
			 * Preparation
			 */
			// Create chunks
			sizes = new int[(int) m_chunksPerThread];
			for (int i = 0; i < m_chunksPerThread; i++) {
				sizes[i] = Tools.getRandomValue(m_minChunkSize, m_maxChunkSize);
			}
			chunks = Core.createNewChunks(sizes);
			for (int i = 0; i < m_chunksPerThread; i++) {
				chunks[i].getData().put(("This is a test! (" + m_nodeID + ")").getBytes());
			}

			// Create list for updates (chunks)
			chunkList = new ArrayList<Chunk>();
			for (int i = 0; i < m_chunksPerThread; i++) {
				chunkList.add(chunks[i]);
			}
			Collections.shuffle(chunkList);
			updates = chunkList.subList(0, m_updates).toArray(new Chunk[m_updates]);

			// Create list for deletes (chunkIDs)
			chunkIDList = new ArrayList<Long>();
			for (int i = 0; i < m_chunksPerThread; i++) {
				chunkIDList.add(chunks[i].getChunkID());
			}
			Collections.shuffle(chunkIDList);
			removes = chunkIDList.subList(0, m_deletes).stream().mapToLong(l -> l).toArray();

			// Create fill chunks (to clear secondary log buffer)
			fillChunks = Core.createNewChunks(new int[] {1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576});

			if (TEST_LOCALLY) {
				log = CoreComponentFactory.getLogInterface();
				log.initBackupRangeLocallyTEST();
			}

			/*
			 * Execution
			 */
			// Put
			System.out.print("Starting replication...");
			if (TEST_LOCALLY) {
				for (Chunk chunk : chunks) {
					chunk.incVersion();
					log.logChunkLocallyTEST(chunk);
				}
			} else {
				Core.put(chunks);
			}
			System.out.println("done\n");

			// Updates
			System.out.print("Starting updates...");
			if (TEST_LOCALLY) {
				for (Chunk chunk : updates) {
					chunk.incVersion();
					log.logChunkLocallyTEST(chunk);
				}
			} else {
				Core.put(updates);
			}
			System.out.println("done\n");

			// Delete
			System.out.print("Starting deletion...");
			if (TEST_LOCALLY) {
				for (long chunkID : removes) {
					log.removeChunkLocallyTEST(chunkID, 1000);
				}
			} else {
				Core.remove(removes);
			}
			System.out.println("done\n");

			// Put dummies
			System.out.print("Starting fill replication...");
			if (TEST_LOCALLY) {
				for (Chunk chunk : fillChunks) {
					log.logChunkLocallyTEST(chunk);
				}
			} else {
				Core.put(fillChunks);
			}
			System.out.println("done");

		} catch (final DXRAMException e) {
			System.out.println("Error: Could not create, update or delete chunk");
		}
	}
}
