{
	"m_numRequiredSlaves": 0,
	"m_name": "ChunkLocalPaperBenchRemove16",
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
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 1 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 1,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 2 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 2,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 4,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 6 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 4 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 6,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 8 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 8,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 12 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 12,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 1"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 1,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 2"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 2,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 4"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 4,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 6"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 6,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 8"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 8,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "######## 16 byte Objects, 16 Thread(s), Batch count 10"
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
		        "m_value": 16,
        		"m_unit": "b"
			},
			"m_pattern": 0
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.bench.ChunkRemoveAllTask",
			"m_numThreads": 16,
			"m_chunkBatch": 10,
			"m_pattern": 0
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		},
		{
			"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.DummyTask",
			"m_comment": "----------------------------------------------------------------------------------------------"
		}
	]
}
