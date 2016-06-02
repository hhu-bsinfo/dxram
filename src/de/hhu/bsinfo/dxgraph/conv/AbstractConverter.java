
package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;

import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Base class for all graph converters.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
abstract class AbstractConverter extends AbstractMain {
	private static final Argument ARG_INPUT = new Argument("in", null, false, "Input file of specific format");
	private static final Argument ARG_INPUT_ROOTS =
			new Argument("inRoots", null, true, "Input file of specific format with BFS roots");
	private static final Argument ARG_OUTPUT =
			new Argument("out", "./", true, "Ordered edge list output file location");
	private static final Argument ARG_FILE_COUNT =
			new Argument("outFileCount", "1", true, "Split data into multiple files (each approx. same size)");
	private static final Argument ARG_INPUT_DIRECTED_EDGES = new Argument("inputDirectedEdges", "true", true,
			"Specify if the input file contains directed or undirected edges");
	private static final Argument ARG_NUM_CONV_THREADS =
			new Argument("numConvThreads", "4", true, "Number of threads converting the data");
	private static final Argument ARG_MAX_BUFFER_QUEUE_SIZE =
			new Argument("maxBufferQueueSize", "100000", true, "Max size of the buffer queue for the file reader");

	private VertexStorage m_storage;
	private boolean m_isDirected;
	private int m_numConverterThreads = -1;
	private int m_maxBufferQueueSize = -1;
	private BinaryEdgeBuffer m_sharedBuffer;
	private ArrayList<AbstractFileReaderThread> m_fileReaderThreads = new ArrayList<>();
	private ArrayList<BufferToStorageThread> m_converterThreads = new ArrayList<>();
	private ArrayList<AbstractFileWriterThread> m_fileWriterThreads = new ArrayList<>();

	/**
	 * Constructor
	 * @param p_description
	 *            Description for the converter.
	 */
	AbstractConverter(final String p_description) {
		super(p_description);
	}

	@Override
	protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_INPUT);
		p_arguments.setArgument(ARG_INPUT_ROOTS);
		p_arguments.setArgument(ARG_OUTPUT);
		p_arguments.setArgument(ARG_FILE_COUNT);
		p_arguments.setArgument(ARG_INPUT_DIRECTED_EDGES);
		p_arguments.setArgument(ARG_NUM_CONV_THREADS);
		p_arguments.setArgument(ARG_MAX_BUFFER_QUEUE_SIZE);
	}

	@Override
	protected int main(final ArgumentList p_arguments) {
		String inputPath = p_arguments.getArgumentValue(ARG_INPUT, String.class);
		String inputRootsPath = p_arguments.getArgumentValue(ARG_INPUT_ROOTS, String.class);
		String outputPath = p_arguments.getArgumentValue(ARG_OUTPUT, String.class);
		int fileCount = p_arguments.getArgumentValue(ARG_FILE_COUNT, Integer.class);
		m_isDirected = p_arguments.getArgumentValue(ARG_INPUT_DIRECTED_EDGES, Boolean.class);
		m_numConverterThreads = p_arguments.getArgumentValue(ARG_NUM_CONV_THREADS, Integer.class);
		m_maxBufferQueueSize = p_arguments.getArgumentValue(ARG_MAX_BUFFER_QUEUE_SIZE, Integer.class);

		m_sharedBuffer = new BinaryEdgesRingBuffer(m_maxBufferQueueSize);

		m_storage = createVertexStorageInstance();

		System.out.println("Parsing input " + inputPath + "...");

		int ret = parse(inputPath);
		if (ret != 0) {
			System.out.println("Parsing " + inputPath + " failed: " + ret);
			return ret;
		}

		System.out.println("Parsing done, " + m_storage.getTotalVertexCount() + " vertices and "
				+ m_storage.getTotalEdgeCount() + " edges");

		dumpToFiles(outputPath, fileCount);

		System.out.println("Done converting, output in " + outputPath);

		if (inputRootsPath != null) {
			System.out.println("Converting roots list...");
			convertBFSRootList(outputPath, inputRootsPath, m_storage);
			System.out.println("Converting roots list done");
		}

		return 0;
	}

	/**
	 * Provide the vertex storage instance you want to use for the conversion process.
	 * @return VertexStorage instance to use.
	 */
	protected abstract VertexStorage createVertexStorageInstance();

	/**
	 * Create the file reader thread implementation to use.
	 * @param p_inputPath
	 *            Input graph file.
	 * @param p_buffer
	 *            Shared buffer accross all threads to use for buffering input.
	 * @return FileReader instance to use.
	 */
	protected abstract AbstractFileReaderThread createReaderInstance(final String p_inputPath,
			final BinaryEdgeBuffer p_buffer);

	/**
	 * Create a writer instance for outputting the converted data.
	 * @param p_outputPath
	 *            Output file to write to.
	 * @param p_id
	 *            Id of the writer (0 based index).
	 * @param p_idRangeStartIncl
	 *            Range of vertex ids to write to the file, start.
	 * @param p_idRangeEndExcl
	 *            Range of the vertex ids to write the file, end.
	 * @param p_storage
	 *            Storage to access for vertex data to write to the file.
	 * @return FileWriter instance to use.
	 */
	protected abstract AbstractFileWriterThread createWriterInstance(final String p_outputPath, final int p_id,
			final long p_idRangeStartIncl, final long p_idRangeEndExcl, final VertexStorage p_storage);

	/**
	 * Convert the provided bfs root list to the desired representation.
	 * @param p_outputPath
	 *            Output path to write the converter list to.
	 * @param p_inputRootFile
	 *            Input bfs root list file.
	 * @param p_storage
	 *            VertexStorage to use for re-basing the roots.
	 */
	protected abstract void convertBFSRootList(final String p_outputPath, final String p_inputRootFile,
			final VertexStorage p_storage);

	/**
	 * Parse and read the input graph data.
	 * @param p_inputPaths
	 *            List of filepaths with graph input data.
	 * @return Error code of the operation.
	 */
	private int parse(final String... p_inputPaths) {

		System.out.println("Starting file reader threads...");

		for (String inputPath : p_inputPaths) {
			AbstractFileReaderThread thread =
					createReaderInstance(inputPath, m_sharedBuffer);
			thread.start();
			m_fileReaderThreads.add(thread);
		}

		System.out.println("Starting converter threads...");

		for (int i = 0; i < m_numConverterThreads; i++) {
			BufferToStorageThread thread = new BufferToStorageThread(i, m_storage, m_isDirected, m_sharedBuffer);
			thread.start();
			m_converterThreads.add(thread);
		}

		System.out.println("Waiting for file reader threads to finish...");

		// wait for file readers to finish and notify buffer threads afterwards
		for (AbstractFileReaderThread thread : m_fileReaderThreads) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
				System.out.println("Joining thread " + thread + " interrupted");
				return -9;
			}
			if (thread.getErrorCode() != 0) {
				return thread.getErrorCode();
			}
		}

		System.out.println("Waiting for buffer threads to finish...");

		for (BufferToStorageThread thread : m_converterThreads) {
			thread.setRunning(false);
		}

		for (BufferToStorageThread thread : m_converterThreads) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
				System.out.println("Joining thread " + thread + " interrupted");
				return -10;
			}
		}

		return 0;
	}

	/**
	 * Dump the cached graph data to output files.
	 * @param p_outputPath
	 *            Path to write the output files to.
	 * @param p_fileCount
	 *            Number of files to split the graph into.
	 */
	private void dumpToFiles(final String p_outputPath, final int p_fileCount) {
		// adjust output path
		String outputPath = p_outputPath;

		if (!outputPath.endsWith("/")) {
			outputPath += "/";
		}

		// also equals vertex count
		long vertexCount = m_storage.getTotalVertexCount();
		long rangeStart;
		long rangeEnd;
		long processed = 0;

		System.out.println("Dumping " + vertexCount + " vertices to " + p_fileCount + " files...");

		for (int i = 0; i < p_fileCount; i++) {
			rangeStart = processed;
			rangeEnd = rangeStart + (vertexCount / p_fileCount);
			if (rangeEnd >= vertexCount) {
				rangeEnd = vertexCount;
			}

			AbstractFileWriterThread thread = createWriterInstance(outputPath, i, rangeStart, rangeEnd, m_storage);
			thread.start();
			m_fileWriterThreads.add(thread);

			processed += rangeEnd - rangeStart;
		}

		// wait for all writers to finish
		for (AbstractFileWriterThread thread : m_fileWriterThreads) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
				System.out.println("Joining thread " + thread + " failed.");
			}
		}
	}
}
