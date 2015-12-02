package de.uniduesseldorf.dxcompute.test;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

import de.uniduesseldorf.dxcompute.DXCompute;
import de.uniduesseldorf.dxcompute.StorageDummy;

public class DXComputeTest 
{
	public static void main(String[] args) throws DXRAMException
	{
		DXCompute dxCompute = new DXCompute(new LoggerTest());
		dxCompute.init(4, new StorageDummy());
		
		dxCompute.execute(new TaskPipelineTest(), null);
		
		dxCompute.shutdown();
	}
}
