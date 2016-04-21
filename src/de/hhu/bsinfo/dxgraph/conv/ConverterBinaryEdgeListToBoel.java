
package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public class ConverterBinaryEdgeListToBoel extends BinaryEdgeListTo {
	/**
	 * Main entry point.
	 * @param p_args
	 *            Console arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new ConverterBinaryEdgeListToBoel();
		main.run(p_args);
	}

	protected ConverterBinaryEdgeListToBoel() {
		super("Convert a binary edge list to an ordered edge list (binary file)");
	}

	@Override
	protected VertexStorage createVertexStorageInstance() {
		return new VertexStorageBinarySimple();
	}

	@Override
	protected FileWriterThread createWriterInstance(final String p_outputPath, final int p_id,
			final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		return new FileWriterBinaryThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}
}
