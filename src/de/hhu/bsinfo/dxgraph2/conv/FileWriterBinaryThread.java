package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FileWriterBinaryThread extends FileWriterThread
{	
	public FileWriterBinaryThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl, final long p_idRangeEndExcl, final VertexStorage p_storage)
	{
		super(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}
	
	@Override
	public void run() {
		try {
			File file = new File(m_outputPath + "out.boel." + m_id);
			if (file.exists())
				file.delete();
			
			File fileInfo = new File(m_outputPath + "out.ioel." + m_id);
			if (fileInfo.exists())
				fileInfo.delete();
			
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			BufferedWriter out2 = new BufferedWriter(new FileWriter(fileInfo));
			if (!dumpOrdered(out, out2, m_idRangeStartIncl, m_idRangeEndExcl))
			{
				System.out.println("Dumping from vertex storage [" + m_idRangeStartIncl + ", " + m_idRangeEndExcl + "] failed.");
				out.close();
				out2.close();
			}
			
			out.close();
			out2.close();
		} catch (IOException e) {
			System.out.println("Dumping to out file failed: " + e.getMessage());
			m_errorCode = -2;
			return;
		}
		
		System.out.println("Dumping [" + m_idRangeStartIncl + ", " + m_idRangeEndExcl + "] to file done");
		m_errorCode = 0;
	}
	
	private boolean dumpOrdered(final DataOutputStream p_file, final BufferedWriter p_infoFile, final long p_rangeStartIncl, final long p_rangeEndExcl)
	{
		// write header (count)
		try {
			p_file.writeLong(p_rangeEndExcl - p_rangeStartIncl);
			p_infoFile.write("Vertices: " + Long.toString(p_rangeEndExcl - p_rangeStartIncl) + "\n");
		} catch (IOException e) {
			return false;
		}
		
		long edgeCount = 0;
		long vertexCount = 0;
		for (long i = p_rangeStartIncl; i < p_rangeEndExcl; i++)
		{
			ConcurrentLinkedQueue<Long> vertexNeighbourList = ((VertexStorageBinary) m_storage).getVertexNeighbourList(i);
			int neighbourCount = vertexNeighbourList.size();
			
			try {
				p_file.writeInt(neighbourCount);
				for (long id : vertexNeighbourList) {
					p_file.writeLong(id);
				}
			} catch (IOException e) {
				return false;
			}
			
			edgeCount += neighbourCount;
			vertexCount++;
			updateProgress("TotalVerticesToFiles " + m_id, vertexCount, p_rangeEndExcl - p_rangeStartIncl);
		} 
		
		try {
			p_infoFile.write("Edges: " + Long.toString(edgeCount) + "\n");
			p_file.flush();
			p_infoFile.flush();
		} catch (IOException e) {
		}
		
		return true;		
	}
	

}
