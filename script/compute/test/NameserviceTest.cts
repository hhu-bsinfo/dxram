{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "NameServiceTest",
	"m_tasks": [
		{
        	"m_task": "de.hhu.bsinfo.dxcompute.bench.NameserviceRegisterTask",
        	"m_numThreads":100,
        	"m_chunkCount": 1000,
        	"m_chunkBatch": 100
        },
        {
          	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.EmptyTask"
        },
        {
       		"m_task": "de.hhu.bsinfo.dxcompute.bench.NameserviceGetChunkIDTask",
       		"m_numThreads":100
        }
	]
}
