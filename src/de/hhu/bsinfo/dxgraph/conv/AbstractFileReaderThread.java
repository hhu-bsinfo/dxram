
package de.hhu.bsinfo.dxgraph.conv;

import java.util.Queue;

import de.hhu.bsinfo.utils.Pair;

/**
 * Converter thread reading data from the input to convert into a buffer.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public abstract class AbstractFileReaderThread extends ConverterThread {
	protected String m_inputPath;
	protected Queue<Pair<Long, Long>> m_bufferQueue;
	protected int m_maxQueueSize = 100000;

	/**
	 * Constructor
	 * @param p_inputPath
	 *            Path of the file to read.
	 * @param p_bufferQueue
	 *            Shared buffer queue to read the data to.
	 * @param p_maxQueueSize
	 *            Max amount of items to add to the queue before blocking.
	 */
	public AbstractFileReaderThread(final String p_inputPath, final Queue<Pair<Long, Long>> p_bufferQueue,
			final int p_maxQueueSize) {
		super("FileReader " + p_inputPath);

		m_inputPath = p_inputPath;
		m_bufferQueue = p_bufferQueue;
		m_maxQueueSize = p_maxQueueSize;
	}

	@Override
	public void run() {
		m_errorCode = parse();
	}

	/**
	 * Implement parsing of the file here.
	 * @return Return code of the parsing process.
	 */
	public abstract int parse();
}
