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
import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxgraph.data.VertexSimple;

/**
 * Implementation reading vertex data from a buffer filled by a separate file reading thread.
 * The vertex data is stored in binary format.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class OrderedEdgeListBinaryFileThreadBuffering extends AbstractOrderedEdgeListThreadBuffering {

    private DataInputStream m_file;
    private long m_position;
    private long m_vertexIDCounter;

    /**
     * Constructor
     * @param p_path
     *            Filepath of the file to read.
     * @param p_bufferLimit
     *            Max vertices to keep buffered.
     * @param p_partitionStartOffset
     *            Offset in the file to start reading at for the selected partition
     * @param p_partitionEndOffset
     *            Offset in the file the partition ends
     * @param p_filterDupEdges
     *            Filter duplicate edges while loading
     * @param p_filterSelfLoops
     *            Filter self loops while loading
     * @param p_graphTotalVertexCount
     *            Total number of vertices of the full graph
     * @param p_startVertexID
     *            ID of the first vertex in the partition
     */
    public OrderedEdgeListBinaryFileThreadBuffering(final String p_path, final int p_bufferLimit, final long p_partitionStartOffset,
            final long p_partitionEndOffset, final boolean p_filterDupEdges, final boolean p_filterSelfLoops, final long p_graphTotalVertexCount,
            final long p_startVertexID) {
        super(p_path, p_bufferLimit, p_partitionStartOffset, p_partitionEndOffset, p_filterDupEdges, p_filterSelfLoops);

        m_vertexIDCounter = p_startVertexID;
    }

    @Override
    protected void setupFile(final String p_path) {
        try {
            RandomAccessFile file = new RandomAccessFile(p_path, "r");
            file.seek(m_partitionStartOffset);
            m_file = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getFD())));
            if (m_partitionEndOffset == Long.MAX_VALUE) {
                m_partitionEndOffset = file.length();
            }
            m_position = m_partitionStartOffset;
        } catch (final FileNotFoundException e) {
            throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
        } catch (final IOException e) {
            throw new RuntimeException("Seeking to position " + m_partitionStartOffset + " on file '" + p_path + "' failed");
        }
    }

    @Override
    protected VertexSimple readFileVertex() {
        VertexSimple vertex = new VertexSimple();

        try {
            if (m_position < m_partitionEndOffset) {
                int count = Integer.reverseBytes(m_file.readInt());
                m_position += Integer.BYTES;
                vertex.setNeighbourCount(count);
                long[] neighbours = vertex.getNeighbours();

                if (m_filterDupEdges && m_filterSelfLoops) {
                    count = 0;
                    for (int i = 0; i < neighbours.length; i++) {
                        long v = Long.reverseBytes(m_file.readLong());

                        if (v != m_vertexIDCounter) {
                            boolean found = false;
                            for (int j = 0; j < i; j++) {
                                if (neighbours[j] == v) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                neighbours[count++] = v;
                            }
                        }
                    }

                    vertex.setNeighbourCount(count);
                } else if (m_filterDupEdges) {
                    count = 0;
                    for (int i = 0; i < neighbours.length; i++) {
                        long v = Long.reverseBytes(m_file.readLong());

                        boolean found = false;
                        for (int j = 0; j < i; j++) {
                            if (neighbours[j] == v) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            neighbours[count++] = v;
                        }
                    }

                    vertex.setNeighbourCount(count);
                } else if (m_filterSelfLoops) {
                    int pos = 0;
                    for (int i = 0; i < neighbours.length; i++) {
                        long v = Long.reverseBytes(m_file.readLong());
                        if (v == m_vertexIDCounter) {
                            count--;
                        } else {
                            neighbours[pos++] = v;
                        }
                    }

                    vertex.setNeighbourCount(count);
                } else {
                    for (int i = 0; i < neighbours.length; i++) {
                        neighbours[i] = Long.reverseBytes(m_file.readLong());
                    }
                }

                m_position += Long.BYTES * count;
                m_vertexIDCounter++;
            } else {
                return null;
            }
        } catch (final IOException e1) {
            return null;
        }

        return vertex;
    }
}
