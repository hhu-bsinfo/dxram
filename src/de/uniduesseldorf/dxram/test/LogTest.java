
package de.uniduesseldorf.dxram.test;

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
	private static int m_numberOfNodes;
	private static int m_minChunkSize;
	private static int m_maxChunkSize;
	private static long m_numberOfChunks;
	private static long m_chunksPerNode;

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
			m_numberOfNodes = Integer.parseInt(p_arguments[0]);
			m_numberOfChunks = Long.parseLong(p_arguments[1]);
			m_chunksPerNode = m_numberOfChunks / m_numberOfNodes;
			m_minChunkSize = Integer.parseInt(p_arguments[2]);
			m_maxChunkSize = Integer.parseInt(p_arguments[3]);
		} else if (p_arguments.length > 4) {
			System.out.println("Too many program arguments");
		} else {
			System.out.println("Missing program arguments (#nodes, number of chunks, "
					+ "minimal chunk size, maximal chunk size)");
		}

		threads = new Thread[m_numberOfNodes];
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));
			log = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			System.out.println("Error during initialization of DXRAM");
		}

		timeStart = System.currentTimeMillis();
		nodes = new short[m_numberOfNodes];
		for (int i = 0; i < m_numberOfNodes; i++) {
			nodes[i] = (short) ((Math.random() * (65536 - 1 + 1)) + 1);

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
		Chunk chunk;
		Random rand;

		System.out.println("I am " + m_id + ", writing " + m_chunksPerNode + " chunks between " + m_minChunkSize
				+ " Bytes and " + m_maxChunkSize + " Bytes");

		rand = new Random();
		try {
			// Create new chunks
			for (int i = 1; i <= m_chunksPerNode; i++) {
				chunk = Core.createNewChunk(rand.nextInt((m_maxChunkSize - m_minChunkSize) + 1) + m_minChunkSize);
				chunk.getData().put(("This is a test! (" + m_nodeID + ")").getBytes());
				Core.put(chunk);
			}
			// Update chunks
			/*
			 * for (int i = 1; i <= m_chunksPerNode; i++) {
			 * chunk = Core.get(((long) m_nodeID << 48) + random);
			 * Core.put(chunk);
			 * }
			 * // Remove chunks
			 * for (int i = 1; i <= m_chunksPerNode; i++) {
			 * Core.remove(((long) m_nodeID << 48) + random);
			 * }
			 */
		} catch (final DXRAMException e) {
			System.out.println("Error: Could not create, update or delete chunk");
		}
	}

}
