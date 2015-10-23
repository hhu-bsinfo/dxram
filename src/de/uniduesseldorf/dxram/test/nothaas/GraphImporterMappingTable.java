package de.uniduesseldorf.dxram.test.nothaas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable;
import de.uniduesseldorf.dxram.core.chunk.storage.PagingTable;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Pair;

public class GraphImporterMappingTable implements GraphImporter
{
	private RandomAccessFile m_edgeFile;
	private PagingTable m_mappingTable;
	
	public GraphImporterMappingTable()
	{
		
	}
	
	@Override
	public void setMappingTable(PagingTable mappingTable)
	{
		m_mappingTable = mappingTable;
	}
	
	@Override
	public boolean setEdgeInputFile(File edgeFile)
	{
		try {
			m_edgeFile = new RandomAccessFile(edgeFile, "r");
		} catch (FileNotFoundException e) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public List<Pair<Long, Long>> readEdges(int numEdges) 
	{
		synchronized (m_edgeFile)
		{
			List<Pair<Long, Long>> ret = new Vector<Pair<Long, Long>>(numEdges);
			
			for (int i = 0; i < numEdges; i++)
			{
				Long fromNode;
				Long toNode;
				
				try
				{
					fromNode = m_edgeFile.readLong();
					toNode = m_edgeFile.readLong();
				}
				catch (IOException e)
				{
					break;
				}
				
				ret.add(new Pair<Long, Long>(fromNode, toNode));
			}
	
			return ret;
		}
	}
	
	@Override
	public long getNumberOfEdges()
	{
		try {
			return m_edgeFile.length() / 16;
		} catch (IOException e) {
			return 0;
		}
	}

	@Override
	public long getChunkIDForNode(long node) 
	{
		try {
			long chunkID = m_mappingTable.get(node);
			//System.out.println("Get: " + node + " -> " + chunkID);
			if (chunkID <= 0)
				return -1;
			else 
				return chunkID;
		} catch (MemoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	@Override
	public void setChunkIDForNode(long node, long chunkID)
	{
		try {
			//System.out.println("Set: " + node + " -> " + chunkID);
			m_mappingTable.set(node, chunkID);
		} catch (MemoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
