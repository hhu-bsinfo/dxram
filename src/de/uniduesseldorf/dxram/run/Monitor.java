
package de.uniduesseldorf.dxram.run;

import java.util.Arrays;
import java.util.Scanner;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Monitoring peer
 * @author Kevin Beineke 13.08.2015
 */
public final class Monitor {

	// Constructors
	/**
	 * Creates an instance of Monitor
	 */
	private Monitor() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		Scanner scanner;
		String command;
		String[] arguments;

		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler
					.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler
							.getConfigurationFromFile("config/nodes.dxram"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

		System.out.println("Monitor started");

		scanner = new Scanner(System.in);
		while (true) {
			System.out.print("Enter command (print 'help' for instructions): ");
			command = scanner.nextLine();

			if (command.equals("help")) {
				System.out.println("A command is built as follows: 'type arg1 arg2 ...'");
				System.out.println("Example: 'migrate CID FROM TO', whereas CID is the ChunkID of the Chunk "
						+ "that is migrated, FROM is the NodeID of the peer the Chunk resides now "
						+ "and TO is the NodeID of the peer the Chunk is sent to.");
			} else {
				arguments = command.split(" ");
				try {
					Core.execute(arguments[0], Arrays.copyOfRange(arguments, 1, arguments.length));
				} catch (final DXRAMException e) {
					scanner.close();
				}
			}
		}
	}

}
