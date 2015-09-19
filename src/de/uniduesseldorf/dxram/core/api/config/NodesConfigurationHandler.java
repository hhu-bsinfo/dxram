
package de.uniduesseldorf.dxram.core.api.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.NodesConfigurationEntry;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Controls the nodes configuration of DXRAM
 * @author Florian Klein 03.09.2013
 */
public final class NodesConfigurationHandler {

	// Constants
	private static final char COMMENT = '#';

	private static final Logger LOGGER = Logger.getLogger(NodesConfigurationHandler.class);

	// Constructors
	/**
	 * Creates an instance of NodesConfigurationHandler
	 */
	private NodesConfigurationHandler() {}

	// Methods
	/**
	 * Creates a empty nodes configuration of DXRAM
	 * @return a empty nodes configuration of DXRAM
	 */
	public static NodesConfiguration getEmptyConfiguration() {
		NodesConfiguration ret;

		ret = new NodesConfiguration();

		return ret;
	}

	/**
	 * Creates a nodes configuration of DXRAM with the local node only
	 * @return a nodes configuration of DXRAM with the local node only
	 */
	public static NodesConfiguration getLocalConfiguration() {
		NodesConfiguration ret;

		ret = getEmptyConfiguration();
		ret.addNode(new NodesConfigurationEntry("127.0.0.1", 22222, (short) 0, (short) 0, Role.SUPERPEER));

		return ret;
	}

	/**
	 * Loads the nodes configuration of DXRAM from a file
	 * @param p_filename
	 *            the filename of the file
	 * @return the nodes configuration of dxram read from file
	 * @throws DXRAMException
	 *             if the nodes configuration file could not be read
	 */
	public static NodesConfiguration getConfigurationFromFile(final String p_filename) throws DXRAMException {
		return getConfigurationFromFile(new File(p_filename));
	}

	/**
	 * Loads the nodes configuration of DXRAM from a file
	 * @param p_file
	 *            the file
	 * @return the nodes configuration of dxram read from file
	 * @throws DXRAMException
	 *             if the nodes configuration file could not be read
	 */
	public static NodesConfiguration getConfigurationFromFile(final File p_file) throws DXRAMException {
		NodesConfiguration ret;

		ret = getEmptyConfiguration();

		try {
			readFile(ret, p_file);
		} catch (final IOException e) {
			throw new DXRAMException("ERR: Could not read from nodes configuration file", e);
		}

		return ret;
	}

	/**
	 * Read properties from a File and stores in given Configuration
	 * @param p_file
	 *            the File
	 * @param p_configuration
	 *            the nodes configuration
	 * @throws IOException
	 *             if the file access fails
	 */
	private static void readFile(final NodesConfiguration p_configuration, final File p_file) throws IOException {
		BufferedReader in;
		String line;
		String[] values;

		in = new BufferedReader(new FileReader(p_file));

		while ((line = in.readLine()) != null) {
			line = line.trim();

			if (line.length() > 0) {
				if (line.charAt(0) == COMMENT) {
					LOGGER.trace("comment line: " + line);
				} else {
					values = line.split("\\s+");
					if (values.length == 5) {
						try {
							p_configuration.addNode(new NodesConfigurationEntry(values[0], Integer.parseInt(values[1]), Short.parseShort(values[3]), Short
									.parseShort(values[4]), Role.getRoleByAcronym(values[2].charAt(0))));
						} catch (final RuntimeException e) {
							in.close();
							throw new IOException("Could not parse line: '" + line + "'", e);
						}
					} else {
						LOGGER.info("corrupt line: " + line);
					}
				}
			}
		}
		in.close();
	}
}
