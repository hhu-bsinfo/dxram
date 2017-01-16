{
	"m_numRequiredSlaves": 0,
	"m_name": "ChunkDataModifyTest",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_getCount": 1000000,
			"m_chunkBatch": 10,
			"m_operation": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_getCount": 1000000,
			"m_chunkBatch": 10,
			"m_operation": 1
		}
	]
}
