package de.hhu.bsinfo.dxgraph.test;

import de.hhu.bsinfo.dxcompute.DXCompute;
import de.hhu.bsinfo.dxcompute.StorageDXRAM;
import de.hhu.bsinfo.dxcompute.TaskPipeline;
import de.hhu.bsinfo.dxcompute.test.LoggerTest;
import de.hhu.bsinfo.dxgraph.bfs.TaskBreadthFirstSearch;
import de.hhu.bsinfo.dxgraph.load.TaskLoadGraphEdgeList;
import de.hhu.bsinfo.dxram.engine.DXRAMException;
import de.hhu.bsinfo.utils.Pair;

public class BreadthFirstSearchTest 
{
	public static void main(String[] args) throws DXRAMException
	{
		DXCompute dxCompute = new DXCompute(new LoggerTest());
		dxCompute.init(1, new StorageDXRAM());
		
		TaskPipeline taskPipeline = new TaskPipeline("BFSTest");
		taskPipeline.pushTask(new TaskLoadGraphEdgeList());
		taskPipeline.pushTask(new TaskBreadthFirstSearch());
		
		dxCompute.execute(taskPipeline, new Pair<String, String>("./edge_list", "./root_list"));

		dxCompute.shutdown();
	}
}
