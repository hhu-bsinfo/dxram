
package de.hhu.bsinfo.dxgraph.conv;

/**
 * Converter thread reading data from the input to convert into a buffer.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
abstract class AbstractFileReaderThread extends ConverterThread {
	protected String m_inputPath;
	protected BinaryEdgeBuffer m_buffer;

	/**
	 * Constructor
	 *
	 * @param p_inputPath Path of the file to read.
	 * @param p_buffer    Shared buffer to read the data to.
	 */
	AbstractFileReaderThread(final String p_inputPath, final BinaryEdgeBuffer p_buffer) {
		super("FileReader " + p_inputPath);

		m_inputPath = p_inputPath;
		m_buffer = p_buffer;
	}

	@Override
	public void run() {
		m_errorCode = parse();
	}

	/**
	 * Implement parsing of the file here.
	 *
	 * @return Return code of the parsing process.
	 */
	public abstract int parse();
}
