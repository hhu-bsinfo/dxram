{
	"m_minSlaves": 0,
	"m_maxSlaves": 0,
	"m_name": "AbortExample",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.RandomReturnValueTask",
			"m_begin": 0,
			"m_end": 1
		},
		{
			"m_cond": "==",
			"m_param": 0,
			"m_true": {
				"m_tasks": [
					{
					  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
						"m_msg": "Branched == 0"
					},
					{
					  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.EmptyTask"
					}
				]
			},
			"m_false": {
				"m_tasks": [
					{
					  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
						"m_msg": "Branched != 0"
					},
					{
						"m_abortMsg": "Aborting execution in != 0 branch"
					}
				]
			}
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxram.ms.tasks.PrintTask",
			"m_msg": "Message after condition"
		}
	]
}
