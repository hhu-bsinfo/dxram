package de.hhu.bsinfo.dxgraph.conv;

/**
 * Interface for a data structure to buffer binary edges read from a binary edge list file.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.05.16
 */
interface BinaryEdgeBuffer {

	/**
	 * Add a binary edge to the buffer. Only a single thread is calling this.
	 *
	 * @param p_val  Source vertex of the edge.
	 * @param p_val2 Target vertex of the edge.
	 * @return True if adding successful, false otherwise (buffer currently full).
	 */
	boolean pushBack(final long p_val, final long p_val2);

	/**
	 * Pop a single edge from the front of the buffer. Multiple threads are calling this.
	 *
	 * @param p_retVals Long array of size 2 to store the edge to.
	 * @return 0 if buffer empty, -1 on failure and 2 on success
	 */
	int popFront(final long[] p_retVals);

	/**
	 * Check if the buffer is empty.
	 *
	 * @return True if empty, false otherwise.
	 */
	boolean isEmpty();
}
