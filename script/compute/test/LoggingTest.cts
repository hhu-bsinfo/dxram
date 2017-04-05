{
	"m_numRequiredSlaves": 2,
	"m_name": "LoggingTest",
	"m_tasks": [
		{
			"m_task": "de.hhu.bsinfo.dxram.chunk.bench.CheckChunkMemRequiredSizeTask",
			"m_minRequiredSize": {
		        "m_value": 7,
        		"m_unit": "gb"
			},
			"m_minRequiredFree": {
		        "m_value": 7000,
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
							  	"m_abortMsg": "Minimum required key value store size: 7 GB"
							}
						]
					}
				},
				{
					"m_caseValue": -2,
					"m_case": {
						"m_tasks": [
							{
						  		"m_abortMsg": "Not enough free key value store memory, min required: 7000 MB"
							}
						]
					}
				}
			]
		},
		{
		    "m_task": "de.hhu.bsinfo.dxram.ms.tasks.GetSlaveIDTask"
		},
		{
			"m_cond": "<=",
			"m_param": 0,
			"m_true": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkCreateTask",
						"m_numThreads": 4,
						"m_chunkCount": 100000000,
						"m_chunkBatch": 10,
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
						"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintMemoryStatusToConsoleTask"
					}
				]
			},
			"m_false": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
					},
					{
						"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
					}
				]
			}
		},
		{
		    "m_task": "de.hhu.bsinfo.dxram.ms.tasks.GetSlaveIDTask"
		},
		{
			"m_cond": "<=",
			"m_param": 0,
			"m_true": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifySequentialTask",
						"m_numThreads": 4,
						"m_chunkBatch": 10,
						"m_iterations": 1,
						"m_pattern": 1
					}
				]
			},
			"m_false": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
					}
				]
			}
		},
		{
		    "m_task": "de.hhu.bsinfo.dxram.ms.tasks.GetSlaveIDTask"
		},
		{
			"m_cond": ">=",
			"m_param": 1,
			"m_true": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.chunk.bench.ChunkDataModifyRandomTask",
						"m_numThreads": 4,
						"m_chunkBatch": 10,
						"m_opCount": 10000000,
						"m_pattern": 5
					}
				]
			},
			"m_false": {
				"m_tasks": [
					{
						"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
					}
				]
			}
		}
	]
}
