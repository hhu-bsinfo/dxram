
package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Multi threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id and outputting a binary ordered edge list.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public final class ConverterBinaryEdgeListToBoel extends AbstractBinaryEdgeListTo {
	/**
	 * Constructor
	 */
	private ConverterBinaryEdgeListToBoel() {
		super("Convert a binary edge list to an ordered edge list (binary file)");
	}

	/**
	 * Main entry point.
	 * @param p_args
	 *            Console arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new ConverterBinaryEdgeListToBoel();
		main.run(p_args);
	}

	@Override
	protected VertexStorage createVertexStorageInstance(final int p_vertexIdOffset) {
		return new VertexStorageBinaryUnsafe(p_vertexIdOffset);
	}

	@Override
	protected AbstractFileWriterThread createWriterInstance(final String p_outputPath, final int p_id,
			final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		return new FileWriterBinaryThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}
}
