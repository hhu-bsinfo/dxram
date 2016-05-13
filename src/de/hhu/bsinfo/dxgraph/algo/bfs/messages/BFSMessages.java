
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

public class BFSMessages {
	public static final byte TYPE = 60;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE = 1;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_MESSAGE = 2;

	/**
	 * Static class
	 */
	private BFSMessages() {};
}
