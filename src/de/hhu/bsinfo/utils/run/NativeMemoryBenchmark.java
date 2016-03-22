package de.hhu.bsinfo.utils.run;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.UnsafeHandler;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.eval.Stopwatch;
import de.hhu.bsinfo.utils.main.Main;

import sun.misc.Unsafe;

public class NativeMemoryBenchmark extends Main
{
	public static void main(final String[] args) {
		Main main = new NativeMemoryBenchmark();
		main.run(args);
	}
	
	protected NativeMemoryBenchmark() {
		super("Benchmarks methods for accessing native/unsafe memory.");
	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		// TODO argument for path
		JNINativeMemory.load("/home/nothaas/Workspace/workspace_dxram/dxram/jni/libJNINativeMemory.so");
		
		int numRuns = 100000;
		
		System.out.println(">>> JNINativeMemory:");
		runJNINativeMemory(numRuns);
		System.out.println(">>> Unsafe:");
		runUnsafe(numRuns);
		
		return 0;
	}
	
	private void runJNINativeMemory(final int p_numRuns)
	{
		Unsafe unsafe = UnsafeHandler.getInstance().getUnsafe();
		Stopwatch stopwatch = new Stopwatch();
		long address;
		int val;
		
		// TODO wrap avg stuff in stopwatch? -> have 4 stopwatches here
		long avgCreate = 0;
		long avgPut = 0;
		long avgGet = 0;
		long avgRemove = 0;
		
		for (int i = 0 ; i < p_numRuns; i++) 
		{
			stopwatch.start();
			address = unsafe.allocateMemory(1024);
			stopwatch.stop();
			avgCreate += stopwatch.getTime();
			//stopwatch.print("Unsafe alloc 1024", true);
			
			stopwatch.start();
			unsafe.putInt(address, 0xAABBCCDD);
			stopwatch.stop();
			avgPut += stopwatch.getTime();
			//stopwatch.print("Unsafe writeInt 0xAABBCCDD", true);
			
			stopwatch.start();
			val = unsafe.getInt(address);
			stopwatch.stop();
			avgGet += stopwatch.getTime();
			//stopwatch.print("Unsafe readInt 0xAABBCCDD", true);
			
			stopwatch.start();
			unsafe.freeMemory(address);
			stopwatch.stop();
			avgRemove += stopwatch.getTime();
			//stopwatch.print("Unsafe free", true);
		}
		
		avgCreate /= p_numRuns;
		avgPut /= p_numRuns;
		avgGet /= p_numRuns;
		avgRemove /= p_numRuns;
		
		System.out.println("avgCreate: " + avgCreate);
		System.out.println("avgPut: " + avgPut);
		System.out.println("avgGet: " + avgGet);
		System.out.println("avgRemove: " + avgRemove);
	}
	
	private void runUnsafe(final int p_numRuns)
	{
		Stopwatch stopwatch = new Stopwatch();
		long address;
		int val;
		
		long avgCreate = 0;
		long avgPut = 0;
		long avgGet = 0;
		long avgRemove = 0;
		
		for (int i = 0 ; i < p_numRuns; i++) 
		{
			stopwatch.start();
			address = JNINativeMemory.alloc(1024);
			stopwatch.stop();
			avgCreate += stopwatch.getTime();
			//stopwatch.print("Unsafe alloc 1024", true);
			
			stopwatch.start();
			JNINativeMemory.writeInt(address, 0xAABBCCDD);
			stopwatch.stop();
			avgPut += stopwatch.getTime();
			//stopwatch.print("Unsafe writeInt 0xAABBCCDD", true);
			
			stopwatch.start();
			val = JNINativeMemory.readInt(address);
			stopwatch.stop();
			avgGet += stopwatch.getTime();
			//stopwatch.print("Unsafe readInt 0xAABBCCDD", true);
			
			stopwatch.start();
			JNINativeMemory.free(address);
			stopwatch.stop();
			avgRemove += stopwatch.getTime();
			//stopwatch.print("Unsafe free", true);
		}
		
		avgCreate /= p_numRuns;
		avgPut /= p_numRuns;
		avgGet /= p_numRuns;
		avgRemove /= p_numRuns;
		
		System.out.println("avgCreate: " + avgCreate);
		System.out.println("avgPut: " + avgPut);
		System.out.println("avgGet: " + avgGet);
		System.out.println("avgRemove: " + avgRemove);
	}
}
