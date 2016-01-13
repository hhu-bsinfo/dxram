package de.hhu.bsinfo.dxcompute.test;

import de.hhu.bsinfo.dxcompute.TaskPipeline;

public class TaskPipelineTest extends TaskPipeline
{

	public TaskPipelineTest() {
		super("TaskPipelineTest");
		pushTask(new TaskTest());
		pushTask(new TaskTest());
	}

}
