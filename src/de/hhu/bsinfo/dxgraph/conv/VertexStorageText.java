
package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.Pair;

/**
 * Extending the vertex storage to support text representation.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
interface VertexStorageText extends VertexStorage {
	/**
	 * Get the neighbor list encoded as text.
	 * @param p_vertexId
	 *            Vertex id to get the neighbor list of.
	 * @return Pair of Vertex id and neighbor list encoded as string.
	 */
	Pair<Long, String> getVertexNeighbourList(final long p_vertexId);
}
