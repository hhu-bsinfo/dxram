
package de.uniduesseldorf.dxram.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogInterface;

/**
 * Test case for the logging interface
 * @author Kevin Beineke
 *         28.07.2014
 */
public final class LogTest implements Runnable {

	// Constants
	private static int m_numberOfThreads;
	private static int m_minChunkSize;
	private static int m_maxChunkSize;
	private static long m_numberOfChunks;
	private static long m_chunksPerThread;

	// Attributes
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

		if (p_arguments.length == 4) {
			m_numberOfThreads = Integer.parseInt(p_arguments[0]);
			m_numberOfChunks = Long.parseLong(p_arguments[1]);
			m_chunksPerThread = m_numberOfChunks / m_numberOfThreads;
			m_minChunkSize = Integer.parseInt(p_arguments[2]);
			m_maxChunkSize = Integer.parseInt(p_arguments[3]);
		} else if (p_arguments.length > 4) {
			System.out.println("Too many program arguments");
		} else {
			System.out.println("Missing program arguments (#threads, number of chunks, " + "minimal chunk size, maximal chunk size)");
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

		/*
		 * timeStart = System.currentTimeMillis();
		 * for (short i = 0; i < m_numberOfNodes; i++) {
		 * try {
		 * log.printMetadataOfAllEntries(nodes[i]);
		 * //log.readAllEntries(nodes[i]);
		 * } catch (final DXRAMException e) {
		 * System.out.println("Error: Could not read entries of node " + nodes[i]);
		 * }
		 * }
		 * System.out.println("All chunks read in " + (System.currentTimeMillis() - timeStart) + "ms");
		 */
	}

	@Override
	public void run() {
		int[] sizes;
		Chunk[] chunks;
		ArrayList<Integer> list;
		Random rand;

		System.out.println("I am " + m_id + ", writing " + m_chunksPerThread + " chunks between " + m_minChunkSize + " Bytes and " + m_maxChunkSize + " Bytes");

		rand = new Random();
		try {

			/*
			 * for (int i = 0; i < m_chunksPerThread; i++) {
			 * chunk = Core.createNewChunk(rand.nextInt((m_maxChunkSize - m_minChunkSize) + 1) + m_minChunkSize);
			 * chunk.getData().put(("This is a test! (" + m_nodeID + ")").getBytes());
			 * Core.put(chunk);
			 * }
			 */

			// Create new chunks
			sizes = new int[(int) m_chunksPerThread];
			for (int i = 0; i < m_chunksPerThread; i++) {
				sizes[i] = rand.nextInt(m_maxChunkSize - m_minChunkSize + 1) + m_minChunkSize;
			}
			chunks = Core.createNewChunk(sizes);
			for (int i = 0; i < m_chunksPerThread; i++) {
				chunks[i].getData().put(("This is a test! (" + m_nodeID + ")").getBytes());
			}

			// Create list for deletion
			list = new ArrayList<Integer>();
			for (int i = 1; i < m_chunksPerThread; i++) {
				list.add(new Integer(i));
			}
			Collections.shuffle(list);

			// Put
			System.out.print("Starting replication...");
			for (int i = 0; i < m_chunksPerThread; i++) {
				Core.put(chunks[i]);
			}
			System.out.println("done");

			// Delete
			System.out.print("Starting deletion...");
			for (int i = 0; i < 100000; i++) {
				Core.remove(chunks[list.get(i)].getChunkID());
			}
			System.out.println("done");

		} catch (final DXRAMException e) {
			System.out.println("Error: Could not create, update or delete chunk");
		}
	}
}
