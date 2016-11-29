/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxgraph.conv;

/**
 * Converter thread reading data from the input to convert into a buffer.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
abstract class AbstractFileReaderThread extends ConverterThread {
    protected String m_inputPath;
    protected BinaryEdgeBuffer m_buffer;

    /**
     * Constructor
     * @param p_inputPath
     *            Path of the file to read.
     * @param p_buffer
     *            Shared buffer to read the data to.
     */
    AbstractFileReaderThread(final String p_inputPath, final BinaryEdgeBuffer p_buffer) {
        super("FileReader " + p_inputPath);

        m_inputPath = p_inputPath;
        m_buffer = p_buffer;
    }

    @Override
    public void run() {
        m_errorCode = parse();
    }

    /**
     * Implement parsing of the file here.
     * @return Return code of the parsing process.
     */
    public abstract int parse();
}
