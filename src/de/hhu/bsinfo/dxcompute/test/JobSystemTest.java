package de.hhu.bsinfo.dxcompute.test;

import java.util.Random;

import de.hhu.bsinfo.dxcompute.job.JobSystem;

public class JobSystemTest 
{
	public static void main(String[] args)
	{
		JobSystem jobSystem = new JobSystem("test", new LoggerTest());
		jobSystem.init(4);
		
		Random ran = new Random();
		for (int i = 0; i < 40; i++)
		{
			jobSystem.submit(new JobTest(i, ran.nextInt(10) * 500));
		}
		
		jobSystem.shutdown();
		
		System.out.println(">>> DONE");
	}
}
