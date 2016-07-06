
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Implementation reading roots buffered from a binary file
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class OrderedEdgeListRootsBinaryFile implements OrderedEdgeListRoots {

	private DataInputStream m_file;

	/**
	 * Constructor
	 *
	 * @param p_path Filepath to read the roots from.
	 */
	public OrderedEdgeListRootsBinaryFile(final String p_path) {

		String file = p_path;

		int lastIndexPath = file.lastIndexOf('/');
		if (lastIndexPath != -1) {
			file = file.substring(lastIndexPath + 1);
		}

		try {
			m_file = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph roots from file '" + p_path + "', does not exist.");
		}
	}

	@Override
	public long getRoot() {
		long value;
		try {
			value = m_file.readLong();
		} catch (final IOException e) {
			return -1;
		}

		return value;
	}

}
