
package de.hhu.bsinfo.dxgraph.conv;

/**
 * Container interface for storing vertices for conversion. This allows
 * us to store vertices with non continuous IDs first before creating
 * a continuous list with vertex IDs
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public interface VertexStorage {
	/**
	 * Always return a vertex for the specified ID. The ID given does not to be a continuous ID and should NOT be the ID
	 * assigned to the vertex. Make sure to generate continuous IDs on your own and assign these to the vertices. Make
	 * sure these IDs are starting at 1
	 * (not 0). Ensure that this function is implemented thread safe
	 *
	 * @param p_hashValue Hash value used in the source representation. If no mapping exists for this so far, create one.
	 * @return Continuous Vertex ID for the hash value which is used for the output graph.
	 */
	long getVertexId(final long p_hashValue);

	/**
	 * Add a neighbor relationship to a vertex.
	 *
	 * @param p_vertexId          Continuous vertex ID of the vertex to add the neighbor to.
	 * @param p_neighbourVertexId Continous vertex ID of the neighbor to add.
	 */
	void putNeighbour(final long p_vertexId, final long p_neighbourVertexId);

	long getNeighbours(final long p_vertexId, final long[] p_buffer);

	/**
	 * Get the total number of vertices stored so far. This equals the highest continuous ID.
	 *
	 * @return Total number of vertices.
	 */
	long getTotalVertexCount();

	/**
	 * Get the total edge count so far.
	 *
	 * @return Total edge count.
	 */
	long getTotalEdgeCount();

	/**
	 * Get the (currently) total amount of memory the internally used data structures consume.
	 *
	 * @return Total memory in bytes for the internal data structures.
	 */
	long getTotalMemoryDataStructures();
}
