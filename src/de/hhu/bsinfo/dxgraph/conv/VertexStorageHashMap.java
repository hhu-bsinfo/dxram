package de.hhu.bsinfo.dxgraph.conv;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxgraph.data.Vertex;

public class VertexStorageHashMap implements VertexStorage {

	private Map<Long, Vertex> m_map = new HashMap<Long, Vertex>();
	private long m_highestID = 0;
	
	public VertexStorageHashMap()
	{
		
	}
	
	@Override
	public Vertex get(long p_id) {
		return m_map.get(p_id);
	}

	@Override
	public void put(Vertex p_vertex) {
		if (p_vertex.getID() > m_highestID)
			m_highestID = p_vertex.getID();
		
		m_map.put(p_vertex.getID(), p_vertex);
	}

	@Override
	public long getHighestID() {
		return m_highestID;
	}

	@Override
	public void dumpOrdered(RandomAccessFile p_file) 
	{
		for (long i = 1; i <= m_highestID; i++)
		{
			Vertex v = m_map.get(i);
			if (v != null)
			{
				String str = new String();
				
				//str += v.getID() + ":";
				
				boolean first = true;
				for (long neighbourId : v.getNeighbours())
				{
					if (first)
						first = false;
					else
						str += ",";
					
					str += neighbourId;
				}
				
				str += "\n";
				
				try {
					p_file.writeBytes(str);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else
				try {
					p_file.writeBytes("\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
