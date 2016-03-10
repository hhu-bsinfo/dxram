package de.hhu.bsinfo.dxgraph.algo.bfs;

public class BulkFifo extends BulkFifoNaive {
	
	public BulkFifo()
	{
		super();
	}
	
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
