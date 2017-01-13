
{
	"m_numRequiredSlaves": 0,
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.RandomReturnValueTask",
			"m_begin": 0,
			"m_end": 1
		},
		{
			"m_cond": "==",
			"m_param": 0,
			"m_true": {
				"m_tasks": [
					{
					  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
						"m_msg": "Branched == 0"
					},
					{
					  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.EmptyTask"
					}
				]
			},
			"m_false": {
				"m_tasks": [
					{
					  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
						"m_msg": "Branched != 0"
					},
					{
					  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.EmptyTask"
					},
					{
					  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.EmptyTask"
					}
				]
			}
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
			"m_msg": "test"
		}
	]
}
