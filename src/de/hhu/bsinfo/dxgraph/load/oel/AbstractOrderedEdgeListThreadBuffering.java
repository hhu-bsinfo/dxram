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

import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.dxgraph.data.VertexSimple;

/**
 * Base class running a buffered reader of vertex data in a separate thread to speed up loading.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
abstract class AbstractOrderedEdgeListThreadBuffering extends Thread implements OrderedEdgeList {

    private int m_bufferLimit = 1000;
    private ConcurrentLinkedQueue<VertexSimple> m_vertexBuffer = new ConcurrentLinkedQueue<>();

    protected long m_partitionStartOffset;
    protected long m_partitionEndOffset;
    protected boolean m_filterDupEdges;
    protected boolean m_filterSelfLoops;

    /**
     * Constructor
     * @param p_path
     *            Filepath to the ordered edge list file.
     * @param p_bufferLimit
     *            Max number of vertices to keep buffered for reading.
     * @param p_partitionStartOffset
     *            Offset in the file to start reading at for the selected partition
     * @param p_partitionEndOffset
     *            Offset in the file the partition ends
     * @param p_filterDupEdges
     *            Filter duplicate edges while loading
     * @param p_filterSelfLoops
     *            Filter self loops while loading
     */
    AbstractOrderedEdgeListThreadBuffering(final String p_path, final int p_bufferLimit, final long p_partitionStartOffset, final long p_partitionEndOffset,
            final boolean p_filterDupEdges, final boolean p_filterSelfLoops) {
        super("OrderedEdgeListFileBuffering " + p_path);
        m_bufferLimit = p_bufferLimit;
        m_partitionStartOffset = p_partitionStartOffset;
        m_partitionEndOffset = p_partitionEndOffset;
        m_filterDupEdges = p_filterDupEdges;
        m_filterSelfLoops = p_filterSelfLoops;

        setupFile(p_path);

        start();
    }

    @Override
    public VertexSimple readVertex() {
        while (true) {
            VertexSimple vertex = m_vertexBuffer.poll();
            if (vertex != null) {
                return vertex;
            }

            if (this.isAlive()) {
                Thread.yield();
            } else {
                return null;
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            // don't flood memory
            if (m_vertexBuffer.size() > m_bufferLimit) {
                Thread.yield();
            }

            VertexSimple vertex = readFileVertex();
            if (vertex == null) {
                return;
            }

            m_vertexBuffer.add(vertex);
        }
    }

    /**
     * Setup the file to read from.
     * @param p_path
     *            Filepath to the ordered edge list file.
     */
    protected abstract void setupFile(final String p_path);

    /**
     * Read a single vertex from the file.
     * @return VertexSimple read from the file or null if eof.
     */
    protected abstract VertexSimple readFileVertex();
}
