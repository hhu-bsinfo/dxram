{
	"m_numRequiredSlaves": 0,
	"m_name": "ChunkLocalBench",
	"m_tasks": [
		{
			"m_task": "de.hhu.bsinfo.dxcompute.bench.CheckChunkMemRequiredSizeTask",
			"m_minRequiredSize": {
		        "m_value": 32,
        		"m_unit": "gb"
			},
			"m_minRequiredFree": {
		        "m_value": 31,
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
							  	"m_abortMsg": "Minimum required key value store size: 32 GB"
							}
						]
					}
				},
				{
					"m_caseValue": -2,
					"m_case": {
						"m_tasks": [
							{
						  		"m_abortMsg": "Not enough free key value store memory, min required: 31 GB"
							}
						]
					}
				}
			]
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Single threaded small chunks (16 - 64 bytes)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 10000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 10000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
				{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Multi threaded small chunks (16 - 64 bytes)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 10000000,
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
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Single threaded medium chunks (128 b - 1 kb)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 128,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
				{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Multi threaded medium chunks (128 b - 1 kb)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 10000000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 128,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Single threaded big chunks (1 kb - 128 kb)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 500000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 128,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
				{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": ">>>>> Multi threaded big chunks (1 kb - 128 kb)..."
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 8,
			"m_chunkCount": 500000,
			"m_chunkBatch": 10,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 128,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintMemoryStatusToConsoleTask"
		}
	]
}
