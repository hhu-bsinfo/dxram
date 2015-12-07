
package de.uniduesseldorf.dxram.test;

import java.nio.ByteBuffer;
import java.util.Random;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Tools;

/*
 * Start-up:
 * 1) Start at least one superpeer: With parameter "superpeer", must also be a superpeer in nodes.config
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
 * Test case for the distributed evaluation for 2015 conference
 * @author Florian Klein
 *         27.12.2014
 */
public final class Konferenz15Test {

	// Constants
	private static final int MIN_SIZE = 1;
	private static final int MAX_SIZE = 100;

	private static final String PROGRESSBAR = "|0%_____10%_______20%_______30%_______40%_______50%_______60%_______70%_______80%_______90%______100%|";

	// Constructors
	/**
	 * Creates an instance of Konferenz15Test
	 */
	private Konferenz15Test() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		long seed;

		if (p_arguments.length == 1 && p_arguments[0].equals("superpeer")) {
			new Superpeer().start();
		} else if ((p_arguments.length == 3 || p_arguments.length == 4) && p_arguments[0].equals("server")) {
			if (p_arguments.length == 4) {
				seed = Long.parseLong(p_arguments[3]);
			} else {
				seed = System.nanoTime();
			}
			new Server(Integer.parseInt(p_arguments[1]), Integer.parseInt(p_arguments[2]), seed).start();
		} else if (p_arguments.length == 1 && p_arguments[0].equals("client")) {
			new Client().start();
		} else {
			System.out.println("Missing or wrong parameters!");
		}
	}

	// Classes
	/**
	 * Represents a superpeer
	 * @author Florian Klein
	 *         27.12.2014
	 */
	private static class Superpeer {

		// Constructors
		/**
		 * Creates an instance of Superpeer
		 */
		Superpeer() {}

		// Methods
		/**
		 * Starts the superpeer
		 */
		public void start() {
			System.out.println("Initialize superpeer");

			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));

				System.out.println("Superpeer initialized");
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}

			System.out.println("Superpeer started");

			while (true) {}
		}

	}

	/**
	 * Represents the server
	 * @author Florian Klein
	 *         03.07.2014
	 */
	private static class Server {

		// Attributes
		private int m_amount;
		private int m_migrations;

		private long m_seed;

		// Constructors
		/**
		 * Creates an instance of Server
		 * @param p_amount
		 *            the amount of Chunks to create
		 * @param p_migrations
		 *            the number of migrations
		 * @param p_seed
		 *            the seed for the random number generator
		 */
		Server(final int p_amount, final int p_migrations, final long p_seed) {
			m_amount = p_amount;
			m_migrations = p_migrations;

			m_seed = p_seed;
		}

		// Methods
		/**
		 * Starts the server
		 */
		public void start() {
			long addMe;
			long time;
			long chunkID;
			Random random;
			Chunk chunk;
			ByteBuffer data;
			int ranges;
			int migrations;
			int size;

			// Initialize DXRAM
			try {
				System.out.println("Initialize server");

				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
				addMe = (long) NodeID.getLocalNodeID() << 48;

				System.out.println("Server initialized");

				System.out.println("Create Index Chunk");
				chunk = Core.createNewChunk(6, "Idx");
				System.out.println("ChunkID: " + Long.toHexString(chunk.getChunkID()));
				data = chunk.getData();
				data.putShort(NodeID.getLocalNodeID());
				data.putInt(m_amount);
				Core.put(chunk);

				// Create Chunks
				System.out.println("Create Chunks");
				System.out.print(PROGRESSBAR + "\n|");
				time = System.nanoTime();
				for (int i = 1; i <= m_amount; i++) {
					chunk = Core.createNewChunk(12);
					Core.put(chunk);
					if (i % (m_amount / 100) == 0) {
						System.out.print("=");
					}
				}
				time = System.nanoTime() - time;
				System.out.println("|");
				System.out.println(m_amount + " Chunks created in " + Tools.readableNanoTime(time));
				System.out.println("Time/Op: " + Tools.readableNanoTime(time / m_amount));

				System.out.println("Migrate Ranges");
				random = new Random(m_seed);
				migrations = m_migrations;
				ranges = 0;
				time = System.nanoTime();
				while (migrations > 0) {
					ranges++;
					size = Math.min(random.nextInt(MAX_SIZE - MIN_SIZE) + MIN_SIZE, migrations);
					chunkID = random.nextInt(m_amount - size) + 1 + addMe;

					if ((short) ranges == -1) {
						Core.migrateRange(chunkID, chunkID + size - 1, (short) (ranges - 1));
					} else {
						Core.migrateRange(chunkID, chunkID + size - 1, (short) ranges);
					}

					migrations -= size;
				}
				time = System.nanoTime() - time;
				System.out.println(m_migrations + " Chunks (" + ranges + " Ranges) migrated in " + Tools.readableNanoTime(time));
				System.out.println("Time/Op: " + Tools.readableNanoTime(time / ranges));

				System.out.println("Server started");
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}

			while (true) {}
		}

	}

	/**
	 * Represents a client
	 * @author Florian Klein
	 *         03.07.2014
	 */
	private static class Client {

		// Constructors
		/**
		 * Creates an instance of Client
		 */
		Client() {}

		// Methods
		/**
		 * Starts the client
		 */
		public void start() {
			Chunk chunk;
			ByteBuffer data;
			int amount;
			long time;
			long addMe;

			// Initialize DXRAM
			try {
				System.out.println("Initialize client");

				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));

				System.out.println("Client initialized");

				System.out.println("Get Index Chunk");
				chunk = Core.get("Idx");
				System.out.println("ChunkID: " + Long.toHexString(chunk.getChunkID()));
				data = chunk.getData();
				addMe = (long) data.getShort() << 48;
				amount = data.getInt();

				System.out.println("Client started");

				System.out.print(PROGRESSBAR + "\n|");
				time = System.nanoTime();
				for (long i = 1; i <= amount; i++) {
					Core.get(i + addMe + 1);
					if (i % (amount / 100) == 0) {
						System.out.print("=");
					}
				}
				time = System.nanoTime() - time;
				System.out.println("|");
				System.out.println(amount + " Chunks get in " + Tools.readableNanoTime(time));
				System.out.println("Time/Op: " + Tools.readableNanoTime(time / amount));

				System.out.println("Client done");
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}
		}

	}

}
