
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class OrderedEdgeListRootsTextFile implements OrderedEdgeListRoots {

	private BufferedReader m_file;

	public OrderedEdgeListRootsTextFile(final String p_path) {

		String file = p_path;

		int lastIndexPath = file.lastIndexOf('/');
		if (lastIndexPath != -1) {
			file = file.substring(lastIndexPath + 1);
		}

		try {
			m_file = new BufferedReader(new FileReader(p_path));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph roots from file '" + p_path + "', does not exist.");
		}
	}

	@Override
	public long getRoot() {
		String line = null;
		try {
			line = m_file.readLine();
		} catch (final IOException e) {
			return -1;
		}

		// eof
		if (line == null) {
			return -1;
		}

		return Long.parseLong(line);
	}

}
