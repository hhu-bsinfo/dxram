
package de.hhu.bsinfo.dxgraph.algo.bfs.front;

/**
 * Extending the naive implementation, this adds a check
 * if the element is already stored to avoid extreme memory footprint
 * when inserting identical values multiple times.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class BulkFifo extends BulkFifoNaive {

    /**
     * Constructor
     */
    public BulkFifo() {
        super();
    }

    /**
     * Constructor
     * @param p_bulkSize
     *            Specify the bulk size for block allocation.
     */
    public BulkFifo(final int p_bulkSize) {
        super(p_bulkSize);
    }

    @Override
    public boolean pushBack(final long p_val) {
        if (contains(p_val)) {
            return false;
        }

        super.pushBack(p_val);
        return true;
    }
}
