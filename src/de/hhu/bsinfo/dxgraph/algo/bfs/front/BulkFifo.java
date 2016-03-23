package de.hhu.bsinfo.dxgraph.algo.bfs.front;

/**
 * Extending the naive implementation, this adds a check
 * if the element is already stored to avoid extreme memory footprint
 * when inserting identical values multiple times.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class BulkFifo extends BulkFifoNaive {
	
	/**
	 * Constructor
	 */
	public BulkFifo()
	{
		super();
	}
	
	/**
	 * Constructor
	 * @param p_bulkSize Specify the bulk size for block allocation.
	 */
	public BulkFifo(final int p_bulkSize)
	{
		super(p_bulkSize);
	}
	
	@Override
	public void pushBack(final long p_val)
	{
		if (contains(p_val))
			return;
			
		super.pushBack(p_val);
	}
	
	/**
	 * Check if the element is already stored.
	 * @param p_val Value to check.
	 * @return True if already stored, false otherwise.
	 */
	private boolean contains(final long p_val)
	{
		int curBlock = m_blockFront;
		int curPos = m_posFront;
		
		do
		{
			int posEnd = m_bulkSize;
			if (curBlock == m_blockBack)
				posEnd = m_posBack;
			
			for (int i = curPos; i < posEnd; i++)
			{
				if (m_chainedFifo[curBlock][i] == p_val)
					return true;
			}
			
			curBlock++;
			curPos = 0;
		}
		while (curBlock < m_blockBack);
		
		return false;
	}
}
