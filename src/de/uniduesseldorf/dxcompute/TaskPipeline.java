package de.uniduesseldorf.dxcompute;

import java.util.Vector;

public class TaskPipeline 
{
	Vector<Task> m_pipeline = new Vector<Task>();
	
	public TaskPipeline()
	{
	
	}
	
	public void pushTask(final Task p_task)
	{
		m_pipeline.add(p_task);
	}
	
	public void execute(final Object p_arg)
	{
		Object arg;
		
		arg = p_arg;
		
		for (Task task : m_pipeline)
			arg = task.execute(arg);
	}
}
