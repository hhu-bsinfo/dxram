{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "ChunkRemoveAllLocalTest",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		}
	]
}
