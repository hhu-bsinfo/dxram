package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

public class BFSMessages {
	public static final byte TYPE = 60;
	public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE = 1;
	public static final byte SUBTYPE_SLAVE_RUNNING = 2;
	public static final byte SUBTYPE_SLAVE_IDLE = 3;
	
	/**
	 * Static class
	 */
	private BFSMessages() {};
}
