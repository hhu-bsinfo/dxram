
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

/**
 * List of messages used for BFS.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.05.16
 */
public final class BFSMessages {
	public static final byte TYPE = 60;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_REQUEST = 1;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_REQUEST = 2;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_RESPONSE = 3;
	public static final byte SUBTYPE_BFS_RESULT_MESSAGE = 4;

	/**
	 * Static class
	 */
	private BFSMessages() {
	}
}
