
package de.hhu.bsinfo.dxram.run.beineke;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/*
 * Start-up:
 * 1) Start at least one superpeer.
 * 2) Optional: Start peers for backup.
 * 3) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 4) Start clients: No parameters
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
		if (p_arguments.length == 2 && p_arguments[0].equals("server")) {
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
	private static class Server {

		// Attributes
		private int m_amount;

		// Constructors
		/**
		 * Creates an instance of Server
		 * @param p_amount
		 *            the amount of Chunks to create
		 */
		Server(final int p_amount) {
			m_amount = p_amount;
		}

		// Methods
		/**
		 * Starts the server
		 */
		public void start() {
			Chunk anchor;
			Chunk[] chunks;

			// Wait a moment
			try {
				Thread.sleep(500);
			} catch (final InterruptedException e) {}

			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);
			final NameserviceService nameService = dxram.getService(NameserviceService.class);

			// Create anchor
			anchor = new Chunk(Long.BYTES * m_amount);
			chunkService.create(anchor);
			nameService.register(anchor, "anc");

			// Create Mails
			chunks = new Chunk[m_amount];
			for (int i = 0; i < m_amount; i++) {
				chunks[i] = new Chunk(1024);
				chunks[i].getData().put(("Mail " + i).getBytes());
			}
			chunkService.create(chunks);
			chunkService.put(chunks);

			// Set the Mailbox-Content
			for (int i = 0; i < chunks.length; i++) {
				anchor.getData().putLong(i * Long.BYTES, chunks[i].getID());
			}
			chunkService.put(anchor);

			System.out.println("Server started");

			/* Migration test */
			int i = 0;
			while (i < 10) {
				// Wait a moment
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
				i++;
			}
			// try {
			// System.out.println("Migrating range(1,5) to " + 960);
			// Core.migrateRange(((long) NodeID.getLocalNodeID() << 48) + 1,
			// ((long) NodeID.getLocalNodeID() << 48) + 5,
			// (short) 960);
			// System.out.println("Migrating object(1) to " + 320);
			// Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 1, (short) 320);
			// Core.execute("migrate", "" + ((long) NodeID.getLocalNodeID() << 48) + 1, "" + Core.getNodeID(), ""
			// + (short) 320);
			// System.out.println("Migrating object(2) to " + (-15999));
			// Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 2, (short) (-15999));
			// System.out.println("Migrating object(3) to " + (-15615));
			// Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 3, (short) (-15615));
			// } catch (final DXRAMException e1) {}

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
		Client() {}

		// Methods
		/**
		 * Starts the client
		 */
		public void start() {
			int i = 0;
			long chunkID;
			long[] chunkIDs;
			ByteBuffer data;
			Chunk anchor = null;
			Chunk chunk;

			// Wait a moment
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}

			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);
			final NameserviceService nameService = dxram.getService(NameserviceService.class);

			System.out.println("Client started");

			chunkID = nameService.getChunkID("anc");
			// TODO: Size?
			anchor = new Chunk(chunkID, 100);
			chunkService.get(anchor);

			data = anchor.getData();
			chunkIDs = new long[data.capacity() / Long.BYTES];
			while (data.remaining() >= Long.BYTES) {
				chunkIDs[i++] = data.getLong();
			}

			chunk = new Chunk(1024);
			// Get the Mailbox-Content
			while (true) {
				System.out.println("----------");

				for (long id : chunkIDs) {
					chunk.setID(id);
					chunkService.get(chunk);
					System.out.println(new String(chunk.getData().array()) + "\t" + chunk.getID());
				}

				// Wait a moment
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}
	}

}
