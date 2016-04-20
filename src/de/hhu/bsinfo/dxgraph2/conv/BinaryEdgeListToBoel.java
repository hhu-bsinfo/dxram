package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 *
 */
public class BinaryEdgeListToBoel extends BinaryEdgeListTo 
{
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		AbstractMain main = new BinaryEdgeListToBoel();
		main.run(args);
	}
	
	protected BinaryEdgeListToBoel() {
		super("Convert a binary edge list to an ordered edge list (binary file)");
	}
	
	@Override
	protected VertexStorage createVertexStorageInstance()
	{
		return new VertexStorageBinarySimple();
	}
	
	@Override
	protected FileWriterThread createWriterInstance(String p_outputPath, int p_id, long p_idRangeStartIncl,
			long p_idRangeEndExcl, VertexStorage p_storage) {
		return new FileWriterBinaryThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}
}
