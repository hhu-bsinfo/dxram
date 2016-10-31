
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Implementation reading roots buffered from a binary file
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class OrderedEdgeListRootsBinaryFile implements OrderedEdgeListRoots {

    private DataInputStream m_file;

    /**
     * Constructor
     * @param p_filePath
     *            Path to the file to read from
     */
    public OrderedEdgeListRootsBinaryFile(final String p_filePath) {

        try {
            m_file = new DataInputStream(new BufferedInputStream(new FileInputStream(p_filePath)));
        } catch (final FileNotFoundException e) {
            throw new RuntimeException("Cannot load graph roots from file '" + p_filePath + "', does not exist.");
        }
    }

    @Override
    public long getRoot() {
        long value;
        try {
            value = Long.reverseBytes(m_file.readLong());
        } catch (final IOException e) {
            return -1;
        }

        return value;
    }

}
