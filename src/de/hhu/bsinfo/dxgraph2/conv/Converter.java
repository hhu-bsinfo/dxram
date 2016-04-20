package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sun.org.apache.xpath.internal.operations.Bool;

import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

public abstract class Converter extends AbstractMain 
{
	private static final Argument ARG_INPUT = new Argument("in", null, false, "Input file of specific format");
	private static final Argument ARG_INPUT_ROOTS = new Argument("inRoots", null, true, "Input file of specific format with BFS roots");
	private static final Argument ARG_OUTPUT = new Argument("out", "./", true, "Ordered edge list output file location");
	private static final Argument ARG_FILE_COUNT = new Argument("outFileCount", "1", true, "Split data into multiple files (each approx. same size)");
	private static final Argument ARG_INPUT_DIRECTED_EDGES = new Argument("inputDirectedEdges", "true", true, "Specify if the input file contains directed or undirected edges");
	private static final Argument ARG_NUM_CONV_THREADS = new Argument("numConvThreads", "4", true, "Number of threads converting the data");
	private static final Argument ARG_MAX_BUFFER_QUEUE_SIZE = new Argument("maxBufferQueueSize", "100000", true, "Max size of the buffer queue for the file reader");
	
	private VertexStorage m_storage = null;
	private boolean m_isDirected = false;
	private int m_numConverterThreads = -1;
	private int m_maxBufferQueueSize = -1;
	private Queue<Pair<Long, Long>> m_sharedBufferQueue = new ConcurrentLinkedQueue<Pair<Long, Long>>();
	private ArrayList<FileReaderThread> m_fileReaderThreads = new ArrayList<FileReaderThread>();
	private ArrayList<BufferToStorageThread> m_converterThreads = new ArrayList<BufferToStorageThread>();
	private ArrayList<FileWriterThread> m_fileWriterThreads = new ArrayList<FileWriterThread>();
	
	protected Converter(final String p_description) {
		super(p_description);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_INPUT);
		p_arguments.setArgument(ARG_INPUT_ROOTS);
		p_arguments.setArgument(ARG_OUTPUT);
		p_arguments.setArgument(ARG_FILE_COUNT);
		p_arguments.setArgument(ARG_INPUT_DIRECTED_EDGES);
		p_arguments.setArgument(ARG_NUM_CONV_THREADS);
		p_arguments.setArgument(ARG_MAX_BUFFER_QUEUE_SIZE);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		String inputPath = p_arguments.getArgumentValue(ARG_INPUT, String.class);
		String inputRootsPath = p_arguments.getArgumentValue(ARG_INPUT_ROOTS, String.class);
		String outputPath = p_arguments.getArgumentValue(ARG_OUTPUT, String.class);
		int fileCount = p_arguments.getArgumentValue(ARG_FILE_COUNT, Integer.class);
		m_isDirected = p_arguments.getArgumentValue(ARG_INPUT_DIRECTED_EDGES, Boolean.class);
		m_numConverterThreads = p_arguments.getArgumentValue(ARG_NUM_CONV_THREADS, Integer.class);
		m_maxBufferQueueSize = p_arguments.getArgumentValue(ARG_MAX_BUFFER_QUEUE_SIZE, Integer.class);
		
		m_storage = createVertexStorageInstance();
		
		System.out.println("Parsing input " + inputPath + "...");
		
		int ret = parse(inputPath);
		if (ret != 0)
		{
			System.out.println("Parsing " + inputPath + " failed: " + ret);
			return ret;
		}
		
		System.out.println("Parsing done, " + m_storage.getTotalVertexCount() + " vertices and " + m_storage.getTotalEdgeCount() + " edges");
		
		dumpToFiles(outputPath, fileCount);
		
		System.out.println("Done converting, output in " + outputPath);
		
		if (inputRootsPath != null)
		{
			System.out.println("Converting roots list...");
			convertBFSRootList(outputPath, inputRootsPath, m_storage);
			System.out.println("Converting roots list done");
		}
		
		return 0;
	}
	
	protected abstract VertexStorage createVertexStorageInstance();
	
	protected abstract FileReaderThread createReaderInstance(String p_inputPath, final Queue<Pair<Long, Long>> p_bufferQueue, final int p_maxQueueSize);
	
	protected abstract FileWriterThread createWriterInstance(final String p_outputPath, final int p_id, final long p_idRangeStartIncl, final long p_idRangeEndExcl, final VertexStorage p_storage);
	
	protected abstract void convertBFSRootList(final String p_outputPath, final String p_inputRootFile, final VertexStorage p_storage);
	
	private int parse(final String... p_inputPaths) {
				
		System.out.println("Starting file reader threads...");
		
		for (String inputPath : p_inputPaths) {
			FileReaderThread thread = createReaderInstance(inputPath, m_sharedBufferQueue, m_maxBufferQueueSize);
			thread.start();
			m_fileReaderThreads.add(thread);
		}
		
		System.out.println("Starting converter threads...");
		
		for (int i = 0; i < m_numConverterThreads; i++) {
			BufferToStorageThread thread = new BufferToStorageThread(i, m_storage, m_isDirected, m_sharedBufferQueue);
			thread.start();
			m_converterThreads.add(thread);
		}
		
		System.out.println("Waiting for file reader threads to finish...");
		
		// wait for file readers to finish and notify buffer threads afterwards
		for (FileReaderThread thread : m_fileReaderThreads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
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
			} catch (InterruptedException e) {
				System.out.println("Joining thread " + thread + " interrupted");
				return -10;
			}
		}
		
		return 0;
	}
	
	private void dumpToFiles(final String p_outputPath, final int p_fileCount)
	{
		// adjust output path
		String outputPath = p_outputPath;
		
		if (!outputPath.endsWith("/"))
			outputPath += "/";
		
		// also equals vertex count
		long vertexCount = m_storage.getTotalVertexCount();
		long rangeStart = 0;
		long rangeEnd = 0;
		long processed = 0;
		
		System.out.println("Dumping " + vertexCount + " vertices to " + p_fileCount + " files...");
		
		for (int i = 0; i < p_fileCount; i++)
		{
			rangeStart = processed;
			rangeEnd = rangeStart + (vertexCount / p_fileCount);
			if (rangeEnd >= vertexCount)
				rangeEnd = vertexCount;
			
			FileWriterThread thread = createWriterInstance(outputPath, i, rangeStart, rangeEnd, m_storage);
			thread.start();
			m_fileWriterThreads.add(thread);
			
			processed += rangeEnd - rangeStart;
		}
		
		// wait for all writers to finish
		for (FileWriterThread thread : m_fileWriterThreads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				System.out.println("Joining thread " + thread + " failed.");
			}
		}
	}
}
