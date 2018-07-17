{
	"m_minSlaves": 2,
	"m_maxSlaves": 0,
	"m_name": "ChunkDataModifyLocalTest",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 10000,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 10000,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 10000,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 10000,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 10000,
			"m_pattern": 7
		}
	]
}
