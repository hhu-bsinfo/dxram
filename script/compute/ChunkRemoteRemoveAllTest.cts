{
	"m_numRequiredSlaves": 0,
	"m_name": "ChunkRemoteRemoveAllTest",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_remote": true
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		}
	]
}
