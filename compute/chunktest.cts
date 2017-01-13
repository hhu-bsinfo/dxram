{
	"m_numRequiredSlaves": 0,
	"m_name": "test",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 1000,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 512,
        		"m_unit": "b"
			}
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.bench.RebootNodeTask"
		}
	]
}
