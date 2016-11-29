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
 * Thread writing the buffered data and converting it to the output file format.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
abstract class AbstractFileWriterThread extends ConverterThread {
    protected String m_outputPath;
    protected int m_id = -1;
    protected long m_idRangeStartIncl = -1;
    protected long m_idRangeEndIncl = -1;
    protected VertexStorage m_storage;

    /**
     * Constructor
     * @param p_outputPath
     *            Output file to write to.
     * @param p_id
     *            Id of the writer (0 based index).
     * @param p_idRangeStartIncl
     *            Range of vertex ids to write to the file, start.
     * @param p_idRangeEndIncl
     *            Range of the vertex ids to write the file, end.
     * @param p_storage
     *            Storage to access for vertex data to write to the file.
     */
    AbstractFileWriterThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl, final long p_idRangeEndIncl,
            final VertexStorage p_storage) {
        super("BinaryFileWriter " + p_id);

        m_outputPath = p_outputPath;
        m_id = p_id;
        m_idRangeStartIncl = p_idRangeStartIncl;
        m_idRangeEndIncl = p_idRangeEndIncl;
        m_storage = p_storage;
    }

    @Override
    public abstract void run();
}
