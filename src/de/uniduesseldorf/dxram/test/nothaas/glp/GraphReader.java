package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.util.Vector;

import de.uniduesseldorf.dxram.utils.Pair;

public interface GraphReader 
{
	int readEdges(int p_instance, int p_totalInstances, Vector<Pair<Long, Long>> p_buffer, int p_count);
}
