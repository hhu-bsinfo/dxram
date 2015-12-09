
package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;

import de.uniduesseldorf.utils.Tools;
import de.uniduesseldorf.utils.config.ConfigurationHandler;

/**
 * Test case for the logging interface
 * @author Kevin Beineke
 *         28.07.2014
 */
public final class NetworkThroughputTest implements Runnable {

	// Attributes
	private static int m_numberOfThreads;
	private static int m_minChunkSize;
	private static int m_maxChunkSize;
	private static long m_numberOfChunks;
	private static long m_chunksPerThread;
	private short m_nodeID;
	private int m_id;

	// Constructors
	/**
	 * Creates an instance of LogTest
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_id
	 *            the thread identifier
	 */
	private NetworkThroughputTest(final short p_nodeID, final int p_id) {
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

		if (p_arguments.length == 4) {
			m_numberOfThreads = Integer.parseInt(p_arguments[0]);
			m_numberOfChunks = Long.parseLong(p_arguments[1]);
			m_chunksPerThread = m_numberOfChunks / m_numberOfThreads;
			m_minChunkSize = Integer.parseInt(p_arguments[2]);
			m_maxChunkSize = Integer.parseInt(p_arguments[3]);

			if (m_chunksPerThread > Integer.MAX_VALUE) {
				System.out.println("Too many chunks per thread! Exiting.");
				System.exit(-1);
			}
		} else if (p_arguments.length > 4) {
			System.out.println("Too many program arguments! Exiting.");
			System.exit(-1);
		} else {
			System.out.println("Missing program arguments (#threads, #chunks, minimal chunk size, maximal chunk size)! Exiting.");
			System.exit(-1);
		}

		threads = new Thread[m_numberOfThreads];
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e) {
			System.out.println("Error during initialization of DXRAM");
		}

		timeStart = System.currentTimeMillis();
		nodes = new short[m_numberOfThreads];
		for (int i = 0; i < m_numberOfThreads; i++) {
			nodes[i] = (short) (Math.random() * (65536 - 1 + 1) + 1);

			threads[i] = new Thread(new NetworkThroughputTest(nodes[i], i));
			threads[i].start();
		}
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
				System.out.println("Error while joining threads");
			}
		}
		System.out.println("All chunks created in " + (System.currentTimeMillis() - timeStart) + "ms");
	}

	@Override
	public void run() {
		Chunk chunk;

		System.out.println("I am " + m_id + ", writing " + m_chunksPerThread + " chunks between " + m_minChunkSize + " Bytes and " + m_maxChunkSize + " Bytes");

		try {
			for (int i = 0; i < m_chunksPerThread; i++) {
				chunk = Core.createNewChunk(Tools.getRandomValue(m_minChunkSize, m_maxChunkSize));
				chunk.getData().put(("This is a test! (" + m_nodeID + ")").getBytes());
				Core.put(chunk);
			}
		} catch (final DXRAMException e) {
			System.out.println("Error: Could not create, update or delete chunk");
		}
	}
}
