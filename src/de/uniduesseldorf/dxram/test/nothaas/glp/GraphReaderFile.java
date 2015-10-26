package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Vector;

import de.uniduesseldorf.dxram.utils.Pair;

public class GraphReaderFile implements GraphReader
{
	private RandomAccessFile m_edgeFile;
	
	public GraphReaderFile(File p_file)
	{
		try {
			m_edgeFile = new RandomAccessFile(p_file, "r");
		} catch (FileNotFoundException e) {
			// TODO user logger?
			System.out.println("File not found: " + p_file);
		}
	}
	
	@Override
	public int readEdges(int p_instance, int p_totalInstances, Vector<Pair<Long, Long>> p_buffer, int p_count) 
	{		
		ByteBuffer buffer = ByteBuffer.allocate(p_count * 16);
		int readBytes = 0;
		
		synchronized (m_edgeFile)
		{
			try {
				readBytes = m_edgeFile.read(buffer.array());
				System.out.println("File pos: (" + m_edgeFile.getFilePointer() + "/" + m_edgeFile.length() + ")");
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

}
