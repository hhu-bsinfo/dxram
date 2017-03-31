/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
