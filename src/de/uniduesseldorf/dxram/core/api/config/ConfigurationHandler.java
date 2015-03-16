package de.uniduesseldorf.dxram.core.api.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationEntry;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Controls the configuration of DXRAM
 * @author Florian Klein 03.09.2013
 */
public final class ConfigurationHandler {

	// Constants
	private static final char COMMENT = '#';
	private static final String KEY_VALUE_SEPERATOR = "=";

	private static final Logger LOGGER = Logger.getLogger(ConfigurationHandler.class);

	// Constructors
	/**
	 * Creates an instance of ConfigurationHandler
	 */
	private ConfigurationHandler() {}

	// Methods
	/**
	 * Creates the default configuration of DXRAM
	 * @return the default configuration of DXRAM
	 */
	public static Configuration getDefaultConfiguration() {
		Configuration ret;

		ret = new Configuration();

		for (ConfigurationEntry<?> entry : ConfigurationConstants.getConfigurationEntries()) {
			ret.setValue(entry.getKey(), entry.getDefaultValue().toString());
		}

		return ret;
	}

	/**
	 * Loads the configuration of DXRAM from a file
	 * @param p_filename
	 *            the filename of the file
	 * @return the configuration of dxram read from file
	 * @throws DXRAMException
	 *             if the configuration file could not be read
	 */
	public static Configuration getConfigurationFromFile(final String p_filename) throws DXRAMException {
		return getConfigurationFromFile(new File(p_filename));
	}

	/**
	 * Loads the configuration of DXRAM from a file
	 * @param p_file
	 *            the file
	 * @return the configuration of dxram read from file
	 * @throws DXRAMException
	 *             if the configuration file could not be read
	 */
	public static Configuration getConfigurationFromFile(final File p_file) throws DXRAMException {
		Configuration ret;

		ret = getDefaultConfiguration();

		try {
			readFile(ret, p_file);
		} catch (final IOException e) {
			throw new DXRAMException("ERR: Could not read from configuration file", e);
		}

		readVMArguments(ret);

		return ret;
	}

	/**
	 * Read properties from a File and stores in given Configuration
	 * @param p_file
	 *            the File
	 * @param p_configuration
	 *            the configuration
	 * @throws IOException
	 *             if the file access fails
	 */
	private static void readFile(final Configuration p_configuration, final File p_file) throws IOException {
		BufferedReader in;
		String line;
		String[] keyValue;

		in = new BufferedReader(new FileReader(p_file));

		while ((line = in.readLine()) != null) {
			line = line.trim();

			if (line.length() > 0) {
				if (line.charAt(0) == COMMENT) {
					LOGGER.trace("comment line: " + line);
				} else {
					keyValue = line.split(KEY_VALUE_SEPERATOR);
					if (keyValue.length == 2) {
						p_configuration.setValue(keyValue[0].trim(), keyValue[1].trim());
					} else {
						LOGGER.info("corrupt line: " + line);
					}
				}
			}
		}
		in.close();
	}

	/**
	 * Read properties from VM
	 * @param p_configuration
	 *            the configuration
	 */
	private static void readVMArguments(final Configuration p_configuration) {
		String[] keyValue;

		keyValue = new String[2];
		keyValue[0] = "network.ip";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			p_configuration.setValue(keyValue[0], keyValue[1]);
		}

		keyValue[0] = "network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			p_configuration.setValue(keyValue[0], keyValue[1]);
		}
	}

}
