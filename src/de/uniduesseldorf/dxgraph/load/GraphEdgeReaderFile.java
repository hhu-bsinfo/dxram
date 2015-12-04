package de.uniduesseldorf.dxgraph.load;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Vector;

import de.uniduesseldorf.dxram.utils.Pair;

public class GraphEdgeReaderFile implements GraphEdgeReader
{
	private RandomAccessFile m_edgeFile;
	private long m_totalNumOfEdges;
	
	public GraphEdgeReaderFile(File p_file) throws IOException
	{
		m_edgeFile = new RandomAccessFile(p_file, "r");
		m_totalNumOfEdges = m_edgeFile.length() / 16;
	}
	
	@Override
	public int readEdges(Vector<Pair<Long, Long>> p_buffer, int p_count) 
	{		
		ByteBuffer buffer = ByteBuffer.allocate(p_count * 16);
		int readBytes = 0;
		
		synchronized (m_edgeFile)
		{
			try {
				readBytes = m_edgeFile.read(buffer.array());
				System.out.println("File pos: (" + m_edgeFile.getFilePointer() + "/" + m_edgeFile.length() + ")");
				if (readBytes == -1)
					return 0;
			} catch (IOException e) {
			}
		}
		
		LongBuffer longBuffer = buffer.asLongBuffer();
		for (int i = 0; i < readBytes / 16; i += 16)
		{
			p_buffer.add(new Pair<Long, Long>(longBuffer.get(), longBuffer.get()));
		}

		return readBytes / 16;
	}

	@Override
	public long getTotalNumberOfEdges() {
		return m_totalNumOfEdges;
	}

}
