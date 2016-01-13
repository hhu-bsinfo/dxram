package de.hhu.bsinfo.dxcompute.test;

import java.util.Random;

import de.hhu.bsinfo.dxcompute.Task;

public class TaskTest extends Task
{
	public TaskTest()
	{
		super("TestTask");
	}

	@Override
	protected Object execute(Object p_arg) 
	{		
		Random ran = new Random();
		for (int i = 0; i < 40; i++)
		{
			getTaskDelegate().submitJob(new JobTest(i, ran.nextInt(10) * 500));
		}
		
		getTaskDelegate().waitForSubmittedJobsToFinish();
		
		System.out.println(">>> DONE");
		
		// TODO Auto-generated method stub
		return null;
	}
}
