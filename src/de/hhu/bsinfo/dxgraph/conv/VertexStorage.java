package de.hhu.bsinfo.dxgraph.conv;

import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxgraph.data.Vertex;

public interface VertexStorage {
	Vertex get(final long p_id);

	void put(final Vertex p_vertex);
	
	long getHighestID();
	
	boolean dumpOrdered(final RandomAccessFile p_file, final long p_rangeStartIncl, final long p_rangeEndExcl);
}
