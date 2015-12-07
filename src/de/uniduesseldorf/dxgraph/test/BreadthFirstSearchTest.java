package de.uniduesseldorf.dxgraph.test;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Pair;

import de.uniduesseldorf.dxcompute.DXCompute;
import de.uniduesseldorf.dxcompute.StorageDXRAM;
import de.uniduesseldorf.dxcompute.TaskPipeline;
import de.uniduesseldorf.dxcompute.test.LoggerTest;
import de.uniduesseldorf.dxgraph.bfs.TaskBreadthFirstSearch;
import de.uniduesseldorf.dxgraph.load.TaskLoadGraphEdgeList;

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
