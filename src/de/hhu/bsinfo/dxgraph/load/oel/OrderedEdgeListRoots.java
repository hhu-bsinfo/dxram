
package de.hhu.bsinfo.dxgraph.load.oel;

/**
 * Interface for an ordered edge list root list providing
 * entry vertex ids for algorithms like BFS.
 * Vertex indices have to start with id 1.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public interface OrderedEdgeListRoots {

	/**
	 * Get the next root from the list. This does not re-base the vertex id.
	 * @return Next root id of the list or -1 if there are no roots left.
	 */
	long getRoot();
}
