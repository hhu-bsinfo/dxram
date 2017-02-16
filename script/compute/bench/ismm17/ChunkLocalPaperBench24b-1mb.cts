{
	"m_numRequiredSlaves": 0,
	"m_name": "ChunkLocalPaperBench24b-1mb",
	"m_tasks": [
		{
			"m_task": "de.hhu.bsinfo.dxcompute.bench.CheckChunkMemRequiredSizeTask",
			"m_minRequiredSize": {
		        "m_value": 56,
        		"m_unit": "gb"
			},
			"m_minRequiredFree": {
		        "m_value": 55,
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
							  	"m_abortMsg": "Minimum required key value store size: 56 GB"
							}
						]
					}
				},
				{
					"m_caseValue": -2,
					"m_case": {
						"m_tasks": [
							{
						  		"m_abortMsg": "Not enough free key value store memory, min required: 55 GB"
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
			"m_msg": "######## 24 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 1000000000,
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
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 32 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 1000000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 32,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 32,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 48 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 1000000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 48,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 48,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 64 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 750000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 64 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 800000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 64,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 128 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 400000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 128,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 128,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 256 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 200000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 256,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 256,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 512 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 100000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 512,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 512,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 1024 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 50000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1024,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1024,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 2048 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 25000000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 2048,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 2048,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 4096 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 12500000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 4096,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 4096,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 8192 byte Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 6250000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 8192,
        		"m_unit": "b"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 8192,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 3125000,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 16,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 16,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 32k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 1562500,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 32,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 32,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 64k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 781250,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 64,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 64,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 128k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 390625,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 128,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 128,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 256k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 195312,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 256,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 256,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 512k Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 97656,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 512,
        		"m_unit": "kb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 512,
        		"m_unit": "kb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 1mb Objects, 12 Thread(s), Batch count 10"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkCreateTask",
			"m_numThreads": 1,
			"m_chunkCount": 48828,
			"m_chunkBatch": 1,
			"m_chunkSizeBytesBegin": {
		        "m_value": 1,
        		"m_unit": "mb"
			},
			"m_chunkSizeBytesEnd": {
		        "m_value": 1,
        		"m_unit": "mb"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkDataModifyTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_opCount": 1000000000,
			"m_writeContentsAndVerify": false,
			"m_pattern": 1
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.ResetChunkMemoryTask"
		}
	]
}
