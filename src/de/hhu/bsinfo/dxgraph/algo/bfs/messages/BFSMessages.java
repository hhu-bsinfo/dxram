
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

public class BFSMessages {
	public static final byte TYPE = 60;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_REQUEST = 1;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_REQUEST = 2;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_RESPONSE = 3;

	/**
	 * Static class
	 */
	private BFSMessages() {};
}
