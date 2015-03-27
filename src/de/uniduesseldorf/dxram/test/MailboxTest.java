package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.ArrayTools;

/*
 * Start-up:
 * 1) Start at least one superpeer: With parameter "superpeer", must also be a superpeer in nodes.dxram
 * 2) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 3) Set serverID to NodeID of server
 * 4) Start clients: No parameters
 * Notes:
 * a) The server's NodeID only changes if the number of first initialized superpeers differs. For one superpeer:
 * serverID = -15999
 * b) Server and clients can be peers and superpeers, but should be peers only for a reasonable allocation of roles in
 * DXRAM
 */
/**
 * Test case for the distributed Chunk handling
 * @author Florian Klein
 *         22.07.2013
 */
public final class MailboxTest {

	// Constructors
	/**
	 * Creates an instance of MailboxTest
	 */
	private MailboxTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 1 && p_arguments[0].equals("superpeer")) {
			new Superpeer().start();
		} else if (p_arguments.length == 2 && p_arguments[0].equals("server")) {
			new Server(Integer.parseInt(p_arguments[1])).start();
		} else {
			new Client().start();
		}
	}

	// Classes
	/**
	 * Represents a superpeer
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static class Superpeer {

		// Constructors
		/**
		 * Creates an instance of Superpeer
		 */
		public Superpeer() {}

		// Methods
		/**
		 * Starts the superpeer
		 */
		public void start() {
			// Initialize DXRAM
			try {
				System.out.println("Superpeer starting...");

				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));

				System.out.println("Superpeer started");
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}

			while (true) {
				try {
					// Wait a moment
					Thread.sleep(5000);
				} catch (final InterruptedException e) {}
			}
		}

	}

	/**
	 * Represents a superpeer
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static class Server {

		// Attributes
		private int m_amount;

		// Constructors
		/**
		 * Creates an instance of Server
		 * @param p_amount
		 *            the amount of Chunks to create
		 */
		public Server(final int p_amount) {
			m_amount = p_amount;
		}

		// Methods
		/**
		 * Starts the server
		 */
		public void start() {
			long[] chunkIDs;
			Chunk chunk;
			byte[] data;
			byte[] buffer;

			// Wait a moment
			try {
				Thread.sleep(500);
			} catch (final InterruptedException e) {}

			// Initialize EPM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			// Create Mails
			chunkIDs = new long[m_amount];
			for (int i = 0;i < m_amount;i++) {
				try {
					chunk = Core.createNewChunk(1024, "" + i);
					chunk.getData().put(("Mail " + i).getBytes());

					Core.put(chunk);

					chunkIDs[i] = chunk.getChunkID();
				} catch (final DXRAMException e) {
					e.printStackTrace();
				}
			}

			// Set the Mailbox-Content
			data = new byte[0];
			for (long chunkID : chunkIDs) {
				buffer = new byte[8];
				buffer[0] = (byte)(chunkID >>> 56);
				buffer[1] = (byte)(chunkID >>> 48);
				buffer[2] = (byte)(chunkID >>> 40);
				buffer[3] = (byte)(chunkID >>> 32);
				buffer[4] = (byte)(chunkID >>> 24);
				buffer[5] = (byte)(chunkID >>> 16);
				buffer[6] = (byte)(chunkID >>> 8);
				buffer[7] = (byte)(chunkID >>> 0);
				data = ArrayTools.concatArrays(data, buffer);
			}

			try {
				// Create anchor
				chunk = new Chunk(NodeID.getLocalNodeID(), 500, data.length);
				chunk.getData().put(data);
				Core.put(chunk);
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}

			System.out.println("Server started");

			/* Migration test */
			/*
			 * int i = 0;
			 * while (i < 10) {
			 * // Wait a moment
			 * try {
			 * Thread.sleep(1000);
			 * } catch (InterruptedException e) {}
			 * i++;
			 * }
			 * try {
			 * //System.out.println("Migrating range(1,5) to " + 960);
			 * //Core.migrateRange(((long)NodeID.getLocalNodeID() << 48) + 1, ((long)NodeID.getLocalNodeID() << 48) + 5,
			 * (short)960);
			 * System.out.println("Migrating object(1) to " + 320);
			 * Core.migrate(((long)NodeID.getLocalNodeID() << 48) + 1, (short)320);
			 * System.out.println("Migrating object(2) to " + (-29119));
			 * Core.migrate(((long)NodeID.getLocalNodeID() << 48) + 2, (short)(-29119));
			 * System.out.println("Migrating object(3) to " + (-15615));
			 * Core.migrate(((long)NodeID.getLocalNodeID() << 48) + 3, (short)(-15615));
			 * System.out.println("Migrating object(4) to " + 960);
			 * Core.migrate(((long)NodeID.getLocalNodeID() << 48) + 4, (short)960);
			 * } catch (DXRAMException e1) {}
			 */
			while (true) {
				// Wait a moment
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}

	}

	/**
	 * Represents a client
	 * @author Florian Klein
	 *         22.07.2013
	 */
	private static class Client {

		// Constructors
		/**
		 * Creates an instance of Client
		 */
		public Client() {}

		// Methods
		/**
		 * Starts the client
		 */
		public void start() {
			Chunk chunk;
			long chunkID;

			// Wait a moment
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}

			// Initialize EPM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			System.out.println("Client started");

			// Get the Mailbox-Content
			while (true) {
				System.out.println("----------");

				for (int i = 0;i < 10;i++) {
					try {
						chunkID = Core.getChunkID("" + i);
						chunk = Core.lock(chunkID);
						Core.unlock(chunkID);
						if (null != chunk) {
							System.out.println(new String(chunk.getData().array()) + "\t" + chunk.getChunkID());
						}
					} catch (final DXRAMException e) {
						e.printStackTrace();
					}
				}

				// Wait a moment
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}

	}

}
