{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "ChunkLocalShortIntegrityTest",
	"m_tasks": [
		{
			"m_task": "de.hhu.bsinfo.dxram.chunk.bench.CheckChunkMemRequiredSizeTask",
			"m_minRequiredSize": {
		        "m_value": 8,
        		"m_unit": "gb"
			},
			"m_minRequiredFree": {
		        "m_value": 7,
        		"m_unit": "gb"
			}
		},
		{
			"m_switchCases": [
				{
					"m_caseValue": -1,
					"m_case": {
						"m_tasks": [
							{
							  	"m_abortMsg": "Minimum required key value store size: 8 GB"
							}
						]
					}
				},
				{
					"m_caseValue": -2,
					"m_case": {
						"m_tasks": [
							{
						  		"m_abortMsg": "Not enough free key value store memory, min required: 7 GB"
							}
						]
					}
				}
			]
		},

	
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Create and remove all 16 bytes sized chunks, single threaded..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 100000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Create and remove all 16 bytes sized chunks, single threaded, batches..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 100000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Create and remove all 16 bytes sized chunks, multi threaded, batches..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 100000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing 16 bytes sized chunks, single threaded, no batches..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 100000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_writeContentsAndVerify": true,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_writeContentsAndVerify": true,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing very small sized chunks: 16 - 64 bytes..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 100000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},

		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing small sized chunks: 64 - 1024 bytes..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 100000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing medium sized chunks: 1 kb - 1 mb..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 10000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1,
        		"m_unit": "mb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing big sized chunks: 1 mb - 64 mb..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 100,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1,
        		"m_unit": "mb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 64,
        		"m_unit": "mb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_opCount": 10000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": ">>>>> Testing huge sized chunks: 64 mb - 256 mb..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 10,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 64,
        		"m_unit": "mb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 256,
        		"m_unit": "mb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_opCount": 1000,
			"m_pattern": 1
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.DummyTask",
			"m_comment": "-----------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
		}
	]
}
