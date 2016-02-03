package de.hhu.bsinfo.dxram.run.test.nothaas.graph500;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class JobCreateEdge extends Job {

	public static final short MS_TYPE_ID = 2;
	static {
		registerType(MS_TYPE_ID, JobCreateEdge.class);
	}
	
	@Override
	public short getTypeID() {
		return MS_TYPE_ID;
	}

	@Override
	protected void execute(short p_nodeID, long[] p_chunkIDs) {
		// chunk 0 contains the generator parameters
		ChunkService chunkService = getService(ChunkService.class);
		LoggerService loggerService = getService(LoggerService.class);
		
		GraphGeneratorParameters graphParameters = new GraphGeneratorParameters(p_chunkIDs[0]);
		if (chunkService.get(graphParameters) != 1)
		{
			loggerService.error(getClass(), "Getting graph generator parameters from chunk " + Long.toHexString(p_chunkIDs[0]) + " failed.");
			return;
		}
		
		// TODO have other job to create distributed graph
		{
			// intervals/ranges [A, B)
			
			long sourceNodeRangeA = 0;
			long sourceNodeRangeB = graphParameters.getVertexCount();
			long destNodeRangeA = 0;
			long destNodeRangeB = graphParameters.getVertexCount();
			
			while ((sourceNodeRangeA + 1 < sourceNodeRangeB) ||
					(destNodeRangeA + 1 < destNodeRangeB))
			{
				// TODO 
				// 1. calculate with probabilities which sector to target
				// 2. adjust sourceNodeRange according to sector hit (if not too small already)
				// 3. adjust destNodeRange according to sector hit (if not too small already)
//			    %% Compare with probabilities and set bits of indices.
//			    ii_bit = rand (1, M) > ab;
//			    jj_bit = rand (1, M) > ( c_norm * ii_bit + a_norm * not (ii_bit) );
//			    ij = ij + 2^(ib-1) * [ii_bit; jj_bit];
			}
			
			// resulting edge sourceNodeRangeA -> destNodeRangeA
			// TODO
			// 1. try to get source node by ID
			// TODO TODO TODO create chunk by ID?
		}
		
	}
	

}
