package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.main.Main;

public class TextEdgeListToOel extends Converter 
{
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new TextEdgeListToOel();
		main.run(args);
	}
	
	protected TextEdgeListToOel() {
		super("Convert a text edge list to an ordered edge list (text file)");
	}
	
	@Override
	protected FileReaderThread createReaderInstance(String p_inputPath,
			ConcurrentLinkedQueue<Pair<Long, Long>> p_bufferQueue, int p_maxQueueSize) {
		return new FileReaderTextThread(p_inputPath, p_bufferQueue, p_maxQueueSize);
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
	
	@Override
	protected void convertBFSRootList(final String p_outputPath, final String p_inputRootFile, final VertexStorage p_storage)
	{
		// TODO
	}

	private static class FileReaderTextThread extends FileReaderThread
	{
		public FileReaderTextThread(String p_inputPath, ConcurrentLinkedQueue<Pair<Long, Long>> p_bufferQueue,
				int p_maxQueueSize) {
			super(p_inputPath, p_bufferQueue, p_maxQueueSize);
		}
		
		@Override
		public int parse() {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(m_inputPath));
			} catch (FileNotFoundException e1) {
				System.out.println("Opening buffered reader failed: " + e1.getMessage());
				return -1;
			}
			long fileSize = 0;
			try {
				RandomAccessFile raf = new RandomAccessFile(m_inputPath, "r");
				fileSize = raf.length();
				raf.close();
			} catch (IOException e2) {
			}
			
			System.out.println("Caching input of edge list " + m_inputPath);

			long lineCount = 0;
			long readByteCount = 0;
			while (true)
			{			
				String line = null;
				try {
					line = reader.readLine();
				} catch (IOException e) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
					System.out.println("Reading line failed: " + e.getMessage());
					return -2;
				}
				
				lineCount++;
				if (line == null) // eof
					break;
				
				readByteCount += line.length() + 1;
				
				String[] tokens = line.split(" ");
				if (tokens.length != 2)
				{
					System.out.println("Invalid token count " + tokens.length + " in line " + lineCount + ", skipping");
					continue;
				}
				
				Long srcNode = Long.parseLong(tokens[0]);
				Long destNode = Long.parseLong(tokens[1]);
				
				m_bufferQueue.add(new Pair<Long, Long>(srcNode, destNode));
				updateProgress("ByteDataPos", readByteCount, fileSize);
			}
			
			try {
				reader.close();
			} catch (IOException e) {
			}
			
			return 0;
		}
	}
}
