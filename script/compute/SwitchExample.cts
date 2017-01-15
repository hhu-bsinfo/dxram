{
	"m_numRequiredSlaves": 0,
	"m_name": "SwitchExample",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.RandomReturnValueTask",
			"m_begin": 0,
			"m_end": 3
		},
		{
			"m_switchCases": [
				{
					"m_caseValue": 0,
					"m_case": {
						"m_tasks": [
							{
							  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
								"m_msg": "Switch case 0"
							}
						]
					}
				},
				{
					"m_caseValue": 1,
					"m_case": {
						"m_tasks": [
							{
							  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
								"m_msg": "Switch case 1"
							}
						]
					}
				}
			],
			"m_switchCaseDefault": {
				"m_case": {
					"m_tasks": [
						{
						  	"m_task": "de.hhu.bsinfo.dxcompute.ms.tasks.PrintTask",
							"m_msg": "Default (optional) case"
						}
					]
				}
			}
		}
	]
}
