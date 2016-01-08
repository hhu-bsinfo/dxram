package de.uniduesseldorf.dxram.core.engine.nodeconfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.boot.NodeRole;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.NodeEntry;

public class NodesConfigurationFileParser implements NodesConfigurationParser {

	// Constants
	private static final char COMMENT = '#';

	private static final Logger LOGGER = Logger.getLogger(NodesConfigurationParser.class);
	
	private File m_file;
	
	public NodesConfigurationFileParser(final File p_file)
	{
		m_file = p_file;
	}
	
	@Override
	public List<NodeEntry> readConfiguration() throws NodesConfigurationException {
		List<NodeEntry> list;
		
		try {
			list = readFile(m_file);
		} catch (final IOException e) {
			throw new NodesConfigurationException("ERR: Could not read from nodes configuration file", e);
		}
		
		return list;
	}

	@Override
	public void writeConfiguration(final List<NodeEntry> p_nodeEntries) throws NodesConfigurationException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented");
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
	private static List<NodeEntry> readFile(final File p_file) throws IOException {
		List<NodeEntry> nodes;
		BufferedReader in;
		String line;
		String[] values;

		nodes = new Vector<NodeEntry>();
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
							nodes.add(new NodeEntry(values[0], Integer.parseInt(values[1]), Short.parseShort(values[3]), Short
									.parseShort(values[4]), NodeRole.getRoleByAcronym(values[2].charAt(0))));
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
		
		return nodes;
	}

}
