package de.uniduesseldorf.utils.config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

public class ConfigurationFileParser implements ConfigurationParser {
	// Constants
	private static final char COMMENT = '#';
	private static final String KEY_VALUE_SEPERATOR = "=";
	
	private static final Logger LOGGER = Logger.getLogger(ConfigurationParser.class);
	
	private File m_file;
	
	public ConfigurationFileParser(final File p_file) {
		m_file = p_file;
	}
	
	@Override
	public void readConfiguration(final Configuration p_configuration) throws ConfigurationException {
		try {
			readFile(p_configuration, m_file);
		} catch (final IOException e) {
			throw new ConfigurationException("ERR: Could not read from configuration file", e);
		}
	}

	@Override
	public void writeConfiguration(final Configuration p_configuration) throws ConfigurationException {
		try {
			writeFile(p_configuration, m_file);
		} catch (final IOException e) {
			throw new ConfigurationException("ERR: Could not write to configuration file", e);
		}
	}
	
	private static void writeFile(final Configuration p_configuration, final File p_file) throws IOException {
		BufferedWriter out;
		
		out = new BufferedWriter(new FileWriter(p_file));

		Set<Entry<String, String>> entries = p_configuration.getValues();
		for (Entry<String, String> entry : entries) {
			out.write(entry.getKey() + " " + KEY_VALUE_SEPERATOR + " " + entry.getValue());
		}
		
		out.close();	
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
}
