
package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public class ConverterBinaryEdgeListToOel extends BinaryEdgeListTo {
	/**
	 * Main entry point.
	 * @param p_args
	 *            Console arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new ConverterBinaryEdgeListToOel();
		main.run(p_args);
	}

	protected ConverterBinaryEdgeListToOel() {
		super("Convert a binary edge list to an ordered edge list (text file)");
	}

	@Override
	protected FileWriterThread createWriterInstance(final String p_outputPath, final int p_id,
			final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		return new FileWriterTextThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}

	@Override
	protected VertexStorage createVertexStorageInstance() {
		return new VertexStorageTextSimple();
	}
}
