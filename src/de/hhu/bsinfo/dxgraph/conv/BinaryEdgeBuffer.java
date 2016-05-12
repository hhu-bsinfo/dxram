package de.hhu.bsinfo.dxgraph.conv;

/**
 * Created by nothaas on 5/11/16.
 */
public interface BinaryEdgeBuffer {

	boolean pushBack(final long p_val, final long p_val2);

	int popFront(final long[] p_retVals);

	boolean isEmpty();
}
