
package de.hhu.bsinfo.dxgraph.load.oel;

import de.hhu.bsinfo.dxgraph.data.Vertex;

/**
 * Interface for an ordered edge list providing vertices.
 * This can be implemented by a file reading with buffering backend.
 * Vertex indices have to start with id 1
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public interface OrderedEdgeList {

	/**
	 * Read vertex data. This does not re-base the vertex id or any ids of the neighbors.
	 * @return Vertex read or null if no vertices are left to read.
	 */
	Vertex readVertex();
}
