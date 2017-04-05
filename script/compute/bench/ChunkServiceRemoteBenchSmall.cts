{
	"m_minSlaves": 2,
	"m_maxSlaves": 0,
	"m_name": "ChunkServiceLocalBench",
	"m_tasks": [
		{
			"m_task": "de.hhu.bsinfo.dxram.chunk.bench.CheckChunkMemRequiredSizeTask",
			"m_minRequiredSize": {
		        "m_value": 1,
        		"m_unit": "gb"
			},
			"m_minRequiredFree": {
		        "m_value": 900,
        		"m_unit": "mb"
			}
		},
		{
			"m_switchCases": [
				{
					"m_caseValue": -1,
					"m_case": {
						"m_tasks": [
							{
							  	"m_abortMsg": "Minimum required key value store size: 1 GB"
							}
						]
					}
				},
				{
					"m_caseValue": -2,
					"m_case": {
						"m_tasks": [
							{
						  		"m_abortMsg": "Not enough free key value store memory, min required: 900 MB"
							}
						]
					}
				}
			]
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 10000000,
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
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 2
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 3
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 4
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 5
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 6
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_opCount": 100000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 7
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		}
	]
}
