package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.Main;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 *
 */
public class BinaryEdgeListToOel extends BinaryEdgeListTo 
{
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new BinaryEdgeListToOel();
		main.run(args);
	}
	
	protected BinaryEdgeListToOel() {
		super("Convert a binary edge list to an ordered edge list (text file)");
	}
	
	@Override
	protected FileWriterThread createWriterInstance(String p_outputPath, int p_id, long p_idRangeStartIncl,
			long p_idRangeEndExcl, VertexStorage p_storage) {
		return new FileWriterTextThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}
	
	@Override
	protected VertexStorage createVertexStorageInstance()
	{
		return new VertexStorageTextSimple();
	}
}
