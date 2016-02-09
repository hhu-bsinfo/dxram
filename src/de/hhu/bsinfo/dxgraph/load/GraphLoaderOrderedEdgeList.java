package de.hhu.bsinfo.dxgraph.load;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.data.ChunkID;

public class GraphLoaderOrderedEdgeList extends GraphLoader {

	@Override
	public boolean load(final String p_path, final int p_numNodes) 
	{		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(p_path, "r");
		} catch (FileNotFoundException e) {
			logError("Cannot load graph from file '" + p_path + "', does not exist.");
			return false;
		}
		
		
		// TODO have setable vertex buffer size
		Vertex[] vertices = new Vertex[100];
		int readCount = 0;
		boolean loop = true;
		while (loop)
		{
			while (readCount < vertices.length)
			{
				Vertex vertex = readVertex(file, ChunkID.getChunkID(getNodeID(), 0));
				if (vertex == null) {
					break;
				}
			
				vertices[readCount] = vertex;
				readCount++;
			}
			
			// create an array which is filled without null padding at the end
			// if necessary 
			if (readCount != vertices.length) {
				Vertex[] tmp = new Vertex[readCount];
				for (int i = 0; i < readCount; i++) {
					tmp[i] = vertices[i];
				}
				
				vertices = tmp;
				loop = false;
			}
			
			if (createData(vertices) != vertices.length)
			{
				// TODO error handling
				System.out.println("error create");
			}
			
			if (putData(vertices) != vertices.length)
			{
				// TODO error handling
				System.out.println("error put");
			}
			
			for (Vertex v : vertices)
			{
				System.out.println(v);
			}
		}		
		
		try {
			file.close();
		} catch (IOException e) {
		}
			
		return true;
	}
	
	private Vertex readVertex(final RandomAccessFile p_file, final long p_idOffset)
	{
		Vertex vertex = new Vertex();
		
		String line;
		try {
			line = p_file.readLine();
		} catch (IOException e) {
			// TODO log error
			return null;
		}
		// eof
		if (line == null)
			return null;
		
		String[] neighbours = line.split(",");
		for (String v : neighbours)
		{
			vertex.getNeighbours().add(Long.parseLong(v) + p_idOffset);
		}

		
		return vertex;
	}
}
