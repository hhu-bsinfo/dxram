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

package de.hhu.bsinfo.dxgraph;

import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.data.Edge;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Special wrapper API (though DXRAMEngine is still accessible) providing
 * graph processing and analysis related functions. This also simplifies
 * or wraps access to certain services to create a common API for graph related
 * tasks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
// TODO broken, needs refactoring and must be adapted to run as a dxram application
public class DXGraph {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXGraph.class.getSimpleName());

    private ChunkService m_chunkService;
    private ChunkRemoveService m_chunkRemoveService;

    /**
     * Constructor
     */
    public DXGraph() {

    }
    //
    //    @Override
    //    protected void postInit() {
    //        m_chunkService = getDXRAMEngine().getService(ChunkService.class);
    //        m_chunkRemoveService = getDXRAMEngine().getService(ChunkRemoveService.class);
    //    }
    //
    //    @Override
    //    protected void preShutdown() {
    //        m_chunkService = null;
    //    }

    /**
     * Create storage on the current node for one or multiple vertices. This assigns
     * a valid ID to each successfully created vertex. The actual
     * data stored with the vertex is not stored with this call.
     *
     * @param p_vertices
     *         Vertices to create storage space for.
     */
    public void createVertices(final Vertex... p_vertices) {
        m_chunkService.create((DataStructure[]) p_vertices);
    }

    /**
     * Create storage on a remote node for one or multiple vertices. This assigns
     * a valid ID to each successfully created vertex. The actual
     * data stored with the vertex is not stored with this call.
     *
     * @param p_nodeId
     *         Node id of another peer to allocate the space on.
     * @param p_vertices
     *         Vertices to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public int createVertices(final short p_nodeId, final Vertex... p_vertices) {
        return m_chunkService.createRemote(p_nodeId, (DataStructure[]) p_vertices);
    }

    /**
     * Create storage on the current node for one or multiple edges. This assigns
     * a valid ID to each successfully created edge. The actual
     * data stored with the edge is not stored with this call.
     *
     * @param p_edges
     *         Edges to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public void createEdges(final Edge... p_edges) {
        m_chunkService.create((DataStructure[]) p_edges);
    }

    /**
     * Create storage on a remote node for one or multiple edges. This assigns
     * a valid ID to each successfully created edge. The actual
     * data stored with the edge is not stored with this call.
     *
     * @param p_nodeId
     *         Node id of another peer to allocate the space on.
     * @param p_edges
     *         VerticEdgeses to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public int createEdges(final short p_nodeId, final Edge... p_edges) {
        return m_chunkService.createRemote(p_nodeId, (DataStructure[]) p_edges);
    }

    /**
     * Write the data of one or multiple vertices to its storage location(s).
     *
     * @param p_vertices
     *         Vertices to write the data to the storage.
     * @return Number of successfully written vertices.
     */
    public int putVertices(final Vertex... p_vertices) {
        return m_chunkService.put((DataStructure[]) p_vertices);
    }

    /**
     * Write the data of one or multiple dges to its storage location(s).
     *
     * @param p_edges
     *         Edges to write the data to the storage.
     * @return Number of successfully written edges.
     */
    public int putEdges(final Edge... p_edges) {
        return m_chunkService.put((DataStructure[]) p_edges);
    }

    /**
     * Read the data of one or multiple vertices from its storage location(s).
     *
     * @param p_vertices
     *         Vertices to read the data from the storage.
     * @return Number of successfully read vertices.
     */
    public int getVertices(final Vertex... p_vertices) {
        return m_chunkService.get((DataStructure[]) p_vertices);
    }

    /**
     * Read the data of one or multiple edges from its storage location(s).
     *
     * @param p_edges
     *         Edges to read the data from the storage.
     * @return Number of successfully read edges.
     */
    public int getEdges(final Edge... p_edges) {
        return m_chunkService.get((DataStructure[]) p_edges);
    }

    /**
     * Delete one or multiple stored vertices from the storage.
     *
     * @param p_vertices
     *         Vertices to delete from storage.
     * @return Number of successfully deleted vertices.
     */
    public int deleteVertices(final Vertex... p_vertices) {
        return m_chunkRemoveService.remove((DataStructure[]) p_vertices);
    }

    /**
     * Delete one or multiple stored edges from the storage.
     *
     * @param p_edges
     *         Edges to delete from storage.
     * @return Number of successfully deleted edges.
     */
    public int deleteEdges(final Edge... p_edges) {
        return m_chunkRemoveService.remove((DataStructure[]) p_edges);
    }

    /**
     * Scan the vertex by getting all its edge objects it is connected to.
     * If the edges are not edge objects but direct connections to the neighbor
     * vertex, this call fails.
     *
     * @param p_vertex
     *         Vertex to scan. If invalid, null is returned.
     * @param p_edgeClass
     *         Class of the edges to return.
     * @param <T>
     *         Type of the edges to create instances of.
     * @return Edge objects of the scanned vertex with their data read from the storage.
     */
    public <T extends Edge> T[] scanEdges(final Vertex p_vertex, final Class<T> p_edgeClass) {
        if (p_vertex.getID() == Vertex.INVALID_ID) {
            return null;
        }

        if (!p_vertex.areNeighborsEdgeObjects()) {
            return null;
        }

        T[] edges = (T[]) Array.newInstance(p_edgeClass, p_vertex.getNeighborCount());
        for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
            try {
                edges[i] = p_edgeClass.newInstance();
                edges[i].setID(p_vertex.getNeighbours()[i]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        m_chunkService.get((DataStructure[]) edges);
        return edges;
    }

    /**
     * Scan the vertex by getting all its neighbor vertices it is connected to.
     * If the edges are actual objects, they are read but skipped automatically.
     *
     * @param p_vertex
     *         Vertex to scan. If invalid, null is returned.
     * @param p_vertexClass
     *         Class of the vertex instances to return.
     * @param <T>
     *         Type of the vertex instances to create.
     * @return Neighbor vertex objects of the scanned vertex with their data stored.
     */
    public <T extends Vertex> T[] scanNeighborVertices(final Vertex p_vertex, final Class<T> p_vertexClass) {
        if (p_vertex.getID() == Vertex.INVALID_ID) {
            return null;
        }

        T[] vertices = (T[]) Array.newInstance(p_vertexClass, p_vertex.getNeighborCount());
        if (p_vertex.areNeighborsEdgeObjects()) {
            // read and skip edges
            Edge[] edges = new Edge[p_vertex.getNeighborCount()];
            for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
                edges[i] = new Edge(p_vertex.getNeighbours()[i]);
            }

            m_chunkService.get((DataStructure[]) edges);

            for (int i = 0; i < edges.length; i++) {
                if (edges[i] != null) {
                    try {
                        vertices[i] = p_vertexClass.newInstance();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    vertices[i].setID(edges[i].getToId());
                }
            }
        } else {
            for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
                if (p_vertex.getNeighbours()[i] != Vertex.INVALID_ID) {
                    try {
                        vertices[i] = p_vertexClass.newInstance();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    vertices[i].setID(p_vertex.getNeighbours()[i]);
                }
            }
        }

        m_chunkService.get((DataStructure[]) vertices);
        return vertices;
    }

    public long traverseBFS(final Vertex p_startVertex, final TraversalVertexCallback p_callback) {
        // returns number of vertices traversed
        // TODO using job system here doesn't seem to be a bad idea
        return 0;
    }

    public interface TraversalVertexCallback {
        // return false to terminate the traversal (because result found, error, ...), true to continue
        boolean evaluateVertex(Vertex p_vertex, int p_depth);
    }

    public interface TraversalEdgeCallback {
        // return false to terminate the traversal (because result found, error, ...), true to continue
        boolean evaluateEdge(Edge p_edge, int p_depth);
    }

    // TODO make this remote executable by registering traversal callback classes
    // and assigning IDs to them for importing/exporting
    // TODO this is doing a single threaded traversal for now...have another job that allows spawning further jobs on traversal (?)
    // and even move the jobs to be executed on remote nodes where the vertex data is available (?) -> job explosion
    // this is a very simple top down only version with a list of next vertices
    private static class TraverseBFSJob extends AbstractJob {

        public static final short MS_TYPE_ID = 1;

        static {
            registerType(MS_TYPE_ID, TraverseBFSJob.class);
        }

        private long m_startVertexId = Vertex.INVALID_ID;
        private TraversalVertexCallback m_vertexCallback;
        private TraversalEdgeCallback m_edgeCallback;
        private Class<? extends Vertex> m_vertexClass;
        private Class<? extends Edge> m_edgeClass;

        /**
         * Constructor
         */
        public TraverseBFSJob(final long p_startVertexId, final TraversalVertexCallback p_vertexCallback, final TraversalEdgeCallback p_edgeCallback,
                final Class<? extends Vertex> p_vertexClass, final Class<? extends Edge> p_edgeClass) {
            m_startVertexId = p_startVertexId;
            m_vertexCallback = p_vertexCallback;
            m_edgeCallback = p_edgeCallback;
            m_vertexClass = p_vertexClass;
            m_edgeClass = p_edgeClass;
        }

        @Override
        public short getTypeID() {
            return MS_TYPE_ID;
        }

        @Override
        protected void execute(final short p_nodeID, final long[] p_chunkIDs) {
            LoggerService logger = getService(LoggerService.class);
            ChunkService chunkService = getService(ChunkService.class);

            // #if LOGGER >= DEBUG
            LOGGER.debug("Starting BFS traversal at 0x%X", m_startVertexId);
            // #endif /* LOGGER >= DEBUG */

            int depth = 0;
            ArrayList<Vertex> current = new ArrayList<>();
            ArrayList<Vertex> next = new ArrayList<>();

            // get root vertex
            try {
                Vertex rootVertex = m_vertexClass.newInstance();
                rootVertex.setID(m_startVertexId);
                if (chunkService.get(rootVertex) != 1) {
                    return;
                }
                current.add(rootVertex);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

            do {
                depth++;

                for (Vertex vertex : current) {
                    if (!evaluateVertex(vertex, depth)) {
                        break;
                    }
                }

                for (Vertex vertex : current) {
                    Vertex[] neighbors;

                    if (vertex.areNeighborsEdgeObjects()) {
                        Edge[] edges = new Edge[vertex.getNeighborCount()];
                        for (int i = 0; i < edges.length; i++) {
                            edges[i].setID(vertex.getNeighbours()[i]);
                        }

                        chunkService.get((DataStructure[]) edges);

                        for (Edge edge : edges) {
                            if (!evaluateEdge(edge, depth)) {
                                break;
                            }
                        }

                        neighbors = new Vertex[edges.length];
                        for (int i = 0; i < neighbors.length; i++) {
                            neighbors[i].setID(edges[i].getToId());
                        }

                    } else {
                        neighbors = new Vertex[vertex.getNeighborCount()];
                        for (int i = 0; i < neighbors.length; i++) {
                            neighbors[i].setID(vertex.getNeighbours()[i]);
                        }
                    }

                    chunkService.get((DataStructure[]) neighbors);
                    for (Vertex neighbor : neighbors) {
                        if (neighbor.getID() != Vertex.INVALID_ID) {
                            next.add(neighbor);
                        }
                    }
                }

            } while (!next.isEmpty());

            // TODO resulting depth? have feature to return result values from jobs
        }

        // -------------------------------------------------------------------

        @Override
        public void importObject(final Importer p_importer) {
            super.importObject(p_importer);
            // TODO
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            super.exportObject(p_exporter);
            // TODO
        }

        @Override
        public int sizeofObject() {
            // TODO
            return super.sizeofObject();
        }

        // -------------------------------------------------------------------

        private boolean evaluateVertex(final Vertex p_vertex, final int p_depth) {
            if (m_vertexCallback != null) {
                return m_vertexCallback.evaluateVertex(p_vertex, p_depth);
            }

            return true;
        }

        private boolean evaluateEdge(final Edge p_edge, final int p_depth) {
            if (m_edgeCallback != null) {
                return m_edgeCallback.evaluateEdge(p_edge, p_depth);
            }

            return true;
        }
    }
}
