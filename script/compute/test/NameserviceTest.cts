{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "NameServiceTest",
	"m_tasks": [
		{
        	"m_task": "de.hhu.bsinfo.dxram.nameservice.bench.NameserviceRegisterTask",
        	"m_numThreads":100,
        	"m_chunkCount": 1000,
        	"m_chunkBatch": 100
        },
        {
          	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
        },
        {
       		"m_task": "de.hhu.bsinfo.dxram.nameservice.bench.NameserviceGetChunkIDTask",
       		"m_numThreads":100
        }
	]
}
