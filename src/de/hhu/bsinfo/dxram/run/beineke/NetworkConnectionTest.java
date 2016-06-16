
package de.hhu.bsinfo.dxram.run.beineke;

import java.util.Iterator;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

/*
 * Start-up:
 * 1) Start at least one superpeer.
 * 2) Optional: Start peers for backup.
 * 3) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 4) Start clients: No parameters
 */
/**
 * Test case for connection creation.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 09.06.2016
 */
public final class NetworkConnectionTest {

	// Constructors
	/**
	 * Creates an instance of NetworkConnectionTest
	 */
	private NetworkConnectionTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		int barrierID = -1;

		// Initialize DXRAM
		final DXRAM dxram = new DXRAM();
		dxram.initialize("config/dxram.conf");
		final ChunkService chunkService = dxram.getService(ChunkService.class);
		final BootService bootService = dxram.getService(BootService.class);
		final NameserviceService nameService = dxram.getService(NameserviceService.class);
		final SynchronizationService synchronizationService = dxram.getService(SynchronizationService.class);

		// Barrier
		if (p_arguments.length == 0) {
			while (barrierID == -1) {
				barrierID = (int) nameService.getChunkID("bar", -1);
			}

			synchronizationService.barrierSignOn(barrierID, -1);
		} else if (p_arguments.length == 1) {
			barrierID = synchronizationService.barrierAllocate(Integer.parseInt(p_arguments[0]));
			nameService.register(barrierID, "bar");

			synchronizationService.barrierSignOn(barrierID, -1);
		} else {
			System.out.println("Too many or negative amount of program arguments!");
		}

		// Broadcast
		Iterator<Short> iter = bootService.getAvailablePeerNodeIDs().iterator();
		while (iter.hasNext()) {
			short nodeID = iter.next();
			chunkService.createRemote(nodeID, 50);
			chunkService.createRemote(nodeID, 50);
			chunkService.createRemote(nodeID, 50);
			chunkService.createRemote(nodeID, 50);
			chunkService.createRemote(nodeID, 50);
		}

		while (true) {
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}
		}

	}

}
