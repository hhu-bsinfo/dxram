{
	"m_minSlaves": 2,
	"m_maxSlaves": 0,
	"m_name": "SimpleTaskScript",
	"m_tasks": [
		{
		  	"m_task": "de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTask",
			"m_pathFile": "../../graph_data/oel/kron_21_16/out.2.ioel"
		},
		{
		  	"m_task": "de.hhu.bsinfo.dxgraph.load.GraphLoadOrderedEdgeListTask",
		  	"m_path": "../../graph_data/oel/kron_21_16",
			"m_vertexBatchSize": 1000,
			"m_filterDupEdges": false,
			"m_filterSelfLoops": false
		},
		{
			"m_task": "de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTask",
			"m_path": "../../graph_data/oel/kron_21_16"
		},
		{
			"m_task": "de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSTask",
			"m_bfsRootNameserviceEntry": "BFS0",
		  	"m_vertexBatchSize": 100,
		  	"m_vertexMessageBatchSize": 100,
		  	"m_numberOfThreadsPerNode": 4,
		  	"m_markVertices": false,
		  	"m_beamerMode": true,
		  	"m_beamerFormulaGraphEdgeDeg": 16,
		  	"m_abortBFSOnError": true
		}
	]
}
