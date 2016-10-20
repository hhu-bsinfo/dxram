
package de.hhu.bsinfo.dxgraph;

/**
 * List of task payloads in the dxgraph package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public final class GraphTaskPayloads {
	public static final short TYPE = 1;
	public static final short SUBTYPE_GRAPH_LOAD_PART_INDEX = 0;
	public static final short SUBTYPE_GRAPH_LOAD_OEL = 1;
	public static final short SUBTYPE_GRAPH_LOAD_BFS_ROOTS = 2;
	public static final short SUBTYPE_GRAPH_ALGO_BFS = 3;

	/**
	 * Static class
	 */
	private GraphTaskPayloads() {}
}
