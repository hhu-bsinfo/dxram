package de.hhu.bsinfo.dxcompute.test;

import de.hhu.bsinfo.dxcompute.DXCompute;
import de.hhu.bsinfo.dxcompute.StorageDummy;
import de.hhu.bsinfo.dxram.engine.DXRAMException;

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
