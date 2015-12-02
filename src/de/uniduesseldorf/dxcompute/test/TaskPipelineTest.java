package de.uniduesseldorf.dxcompute.test;

import de.uniduesseldorf.dxcompute.TaskPipeline;

public class TaskPipelineTest extends TaskPipeline
{

	public TaskPipelineTest() {
		super("TaskPipelineTest");
		pushTask(new TaskTest());
		pushTask(new TaskTest());
	}

}
