package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorMultiLevel;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifo;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifoNaive;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.TreeSetFifo;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.utils.Pair;

public class GraphAlgorithmBFSLocalSingleThreaded extends GraphAlgorithm {

	private int m_vertexBatchCount = 1000;
	private Class<? extends FrontierList> m_frontierListType = null;
	
	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private Vertex2[] m_vertexBatch = null;
	private long m_previousRunParentId = ChunkID.INVALID_ID;
	
	public GraphAlgorithmBFSLocalSingleThreaded(final int p_vertexBatchCount, final Class<? extends FrontierList> p_frontierListType, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_vertexBatchCount = p_vertexBatchCount;
		m_frontierListType = p_frontierListType;
		
		// pool init
		m_vertexBatch = new Vertex2[m_vertexBatchCount];
		for (int i = 0; i < m_vertexBatch.length; i++) {
			m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
		}
	}
	
	@Override
	protected boolean setup() {
		if (m_frontierListType == null || m_frontierListType == BitVector.class)
		{
			m_curFrontier = new BitVector(getGraphLoaderResultDelegate().getTotalVertexCount());
			m_nextFrontier = new BitVector(getGraphLoaderResultDelegate().getTotalVertexCount());
		}
		else if (m_frontierListType == BitVectorMultiLevel.class)
		{
			m_curFrontier = new BitVectorMultiLevel(getGraphLoaderResultDelegate().getTotalVertexCount());
			m_nextFrontier = new BitVectorMultiLevel(getGraphLoaderResultDelegate().getTotalVertexCount());
		}
		else if (m_frontierListType == BulkFifoNaive.class)
		{
			m_curFrontier = new BulkFifoNaive();
			m_nextFrontier = new BulkFifoNaive();
		}
		else if (m_frontierListType == BulkFifo.class)
		{
			m_curFrontier = new BulkFifo();
			m_nextFrontier = new BulkFifo();
		}
		else if (m_frontierListType == TreeSetFifo.class)
		{
			m_curFrontier = new TreeSetFifo();
			m_nextFrontier = new TreeSetFifo();
		}
		else
		{
			throw new RuntimeException("Cannot create instance of FrontierList type " + m_frontierListType);
		}
		return true;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{		
		// execute a full BFS for each node
		int bfsIteration = 0;
		for (long entryNode : p_entryNodes)
		{
			m_loggerService.info(getClass(), "Executing BFS iteration (" + bfsIteration + ") with root node " + Long.toHexString(entryNode));
			m_curFrontier.pushBack(ChunkID.getLocalID(entryNode));
			Pair<Long, Integer> results = runBFS(bfsIteration);
			if (results == null) {
				m_loggerService.error(getClass(), "BFS iteration (" + bfsIteration + ") with root node " + Long.toHexString(entryNode) + " failed.");
				return false;
			} else {
				m_loggerService.info(getClass(), "Results BFS iteration (" + bfsIteration + ") for root node " + Long.toHexString(entryNode) + ": Iteration depth " + results.second() + ", total visited vertices " + results.first());
			}
			
			bfsIteration++;
		}
		
		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private Pair<Long, Integer> runBFS(final int p_bfsIteration)
	{
		int curIterationLevel = 0;
		long visitedCounter = 0;
		short nodeId = m_bootService.getNodeID();
		
		while (true)
		{			
			m_loggerService.debug(getClass(), "Iteration level " + curIterationLevel + " cur frontier size: " + m_curFrontier.size());
			
			boolean iterationLevelDone = false;
			while (!iterationLevelDone)
			{
				int curBatchSize = 0;
				for (int i = 0; i < m_vertexBatchCount; i++) {
					long tmp = m_curFrontier.popFront();
					if (tmp != -1)
					{
						// don't walk back
						if (tmp != m_previousRunParentId) {
							m_vertexBatch[i].setID(ChunkID.getChunkID(nodeId, tmp));
							curBatchSize++;
						}
					}
					else
					{
						iterationLevelDone = true;
					}
				}
			
				int gett = m_chunkService.get(m_vertexBatch, 0, curBatchSize);
				if (gett != curBatchSize)
				{
					m_loggerService.error(getClass(), "Getting vertices failed.");
					return null;
				}
				
				int writeBackCount = 0;
				for (int i = 0; i < gett; i++) {
					// check first if visited
					if (m_vertexBatch[i].getUserData() != p_bfsIteration) {							
						writeBackCount++;
						// set depth level
						m_vertexBatch[i].setUserData(p_bfsIteration);
						visitedCounter++;
						long[] neighbours = m_vertexBatch[i].getNeighbours();
						
						for (long neighbour : neighbours) {								
							// don't walk back
							if (neighbour != m_previousRunParentId) {									
								m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
							}
						}
					} else {
						// already visited, don't have to put back to storage
						m_vertexBatch[i].setID(ChunkID.INVALID_ID);
					}
				}
				
				// write back changes 
				int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, gett);
				if (put != writeBackCount)
				{
					m_loggerService.error(getClass(), "Putting vertices failed.");
					return null;
				}
			}
			
			// swap buffers at the end of the round when iteration level done
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
			m_nextFrontier.reset();
			
			if (m_curFrontier.isEmpty())
			{
				break;
			}
			
			curIterationLevel++;
		}
		
		m_loggerService.info(getClass(), "Iteration depth " + curIterationLevel + ", total visited vertices " + visitedCounter);
		
		return new Pair<Long, Integer>(visitedCounter, curIterationLevel);
	}
}
