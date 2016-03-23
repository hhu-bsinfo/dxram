package de.hhu.bsinfo.utils.run;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.UnsafeHandler;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.eval.EvaluationTable;
import de.hhu.bsinfo.utils.eval.Stopwatch;
import de.hhu.bsinfo.utils.main.Main;

import sun.misc.Unsafe;

/**
 * Benchmark the JNINativeMemory implementation compiled with various
 * compilers and optimization levels and compare it against Java's Unsafe class.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class NativeMemoryBenchmark extends Main
{
	private static final Argument ARG_NUM_RUNS = new Argument("numRuns", "100000", true, "Number of runs to execute for the final result");
	private static final Argument ARG_PATH_JNI_NATIVE_MEMORY = new Argument("pathJNINativeMemory", null, false, "Absolute path to the compiled JNINativeMemory library files");
	
	private EvaluationTable m_table;
	
	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		Main main = new NativeMemoryBenchmark();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	protected NativeMemoryBenchmark() {
		super("Benchmarks methods for accessing native/unsafe memory.");
	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_NUM_RUNS);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		final int numRuns = p_arguments.getArgument(ARG_NUM_RUNS).getValue(Integer.class);
		final String pathJniNativeMemory =  p_arguments.getArgument(ARG_PATH_JNI_NATIVE_MEMORY).getValue(String.class);
		
		mainEval(numRuns, pathJniNativeMemory);
		
		return 0;
	}
	
	/**
	 * Execute evaluation.
	 * @param p_numRuns Number of runs to execute for each implementation.
	 * @param p_pathJniNativeMemory Path to folder with all compiled JNINativeLibraries
	 */
	private void mainEval(final int p_numRuns, final String p_pathJniNativeMemory)
	{
		prepareTableEval();
		
		String path = p_pathJniNativeMemory;
		if (path.lastIndexOf('/') != path.length() - 1) {
			path += "/";
		}
		
		runUnsafe(p_numRuns, "Unsafe");
		
		JNINativeMemory.load(path + "libJNINativeMemory_gcc_o1.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O1");
		JNINativeMemory.load(path + "libJNINativeMemory_gcc_o2.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O2");
		JNINativeMemory.load(path + "libJNINativeMemory_gcc_o3.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-gcc-O3");
		
		JNINativeMemory.load(path + "libJNINativeMemory_clang_o1.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O1");
		JNINativeMemory.load(path + "libJNINativeMemory_clang_o2.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O2");
		JNINativeMemory.load(path + "libJNINativeMemory_clang_o3.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-clang-O3");
		
		JNINativeMemory.load(path + "libJNINativeMemory_icc_o1.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O1");
		JNINativeMemory.load(path + "libJNINativeMemory_icc_o2.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O2");
		JNINativeMemory.load(path + "libJNINativeMemory_icc_o3.so");
		runJNINativeMemory(p_numRuns, "JNINativeMemory-intel-O3");
		
		System.out.println(m_table.toCsv(true, "\t"));
	}
	
	/**
	 * Create the table for measured data
	 */
	private void prepareTableEval()
	{
		m_table = new EvaluationTable(10, 6);
		
		m_table.setIntersectTopCornerName("Memory");
		
		m_table.setRowName(0, "Unsafe");
		m_table.setRowName(1, "JNINativeMemory-gcc-O1");
		m_table.setRowName(2, "JNINativeMemory-gcc-O2");
		m_table.setRowName(3, "JNINativeMemory-gcc-O3");
		m_table.setRowName(4, "JNINativeMemory-clang-O1");
		m_table.setRowName(5, "JNINativeMemory-clang-O2");
		m_table.setRowName(6, "JNINativeMemory-clang-O3");
		m_table.setRowName(7, "JNINativeMemory-intel-O1");
		m_table.setRowName(8, "JNINativeMemory-intel-O2");
		m_table.setRowName(9, "JNINativeMemory-intel-O3");
		
		m_table.setColumnName(0, "create");
		m_table.setColumnName(1, "putInt");
		m_table.setColumnName(2, "getInt");
		m_table.setColumnName(3, "putByteArray");
		m_table.setColumnName(4, "getByteArray");
		m_table.setColumnName(5, "remove");
	}
	
	/**
	 * Execute the benchmark with Java's Unsafe
	 * @param p_numRuns Number of runs to execute.
	 * @param p_rowName Name of the row in the table to put the data into.
	 */
	private void runUnsafe(final int p_numRuns, final String p_rowName)
	{
		Unsafe unsafe = UnsafeHandler.getInstance().getUnsafe();
		Stopwatch[] stopwatches = new Stopwatch[6];
		for (int i = 0; i < stopwatches.length; i++) {
			stopwatches[i] = new Stopwatch();
		}
		
		long address;		
		byte[] data = new byte[1024];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) i;
		}
		
		for (int i = 0 ; i < p_numRuns; i++) 
		{
			stopwatches[0].start();
			address = unsafe.allocateMemory(data.length);
			stopwatches[0].stopAndAccumulate();
			
			stopwatches[1].start();
			unsafe.putInt(address, 0xAABBCCDD);
			stopwatches[1].stopAndAccumulate();
			
			stopwatches[2].start();
			unsafe.getInt(address);
			stopwatches[2].stopAndAccumulate();
			
			stopwatches[3].start();
			for (int j = 0; j < data.length; j++) {
				unsafe.putByte(address + j, data[j]);
			}
			stopwatches[3].stopAndAccumulate();
			
			stopwatches[4].start();
			for (int j = 0; j < data.length; j++) {
				data[j] = unsafe.getByte(address + j);
			}
			stopwatches[4].stopAndAccumulate();
			
			stopwatches[5].start();
			unsafe.freeMemory(address);
			stopwatches[5].stopAndAccumulate();
			
			// sleep to kill performance gains by continuous looping
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		m_table.set(p_rowName, "create", stopwatches[0].getTimeStr());
		m_table.set(p_rowName, "putInt", stopwatches[1].getTimeStr());
		m_table.set(p_rowName, "getInt", stopwatches[2].getTimeStr());
		m_table.set(p_rowName, "putByteArray", stopwatches[3].getTimeStr());
		m_table.set(p_rowName, "getByteArray", stopwatches[4].getTimeStr());
		m_table.set(p_rowName, "remove", stopwatches[5].getTimeStr());
	}
	
	/**
	 * Execute the benchmark with a JNINativeMemory implementation.
	 * @param p_numRuns Number of runs to execute.
	 * @param p_rowName Name of the row in the table to put the data into.
	 */
	private void runJNINativeMemory(final int p_numRuns, final String p_rowName)
	{
		Stopwatch[] stopwatches = new Stopwatch[6];
		for (int i = 0; i < stopwatches.length; i++) {
			stopwatches[i] = new Stopwatch();
		}
		
		long address;		
		byte[] data = new byte[1024];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) i;
		}
		
		for (int i = 0 ; i < p_numRuns; i++) 
		{
			stopwatches[0].start();
			address = JNINativeMemory.alloc(data.length);
			stopwatches[0].stopAndAccumulate();
			
			stopwatches[1].start();
			JNINativeMemory.writeInt(address, 0xAABBCCDD);
			stopwatches[1].stopAndAccumulate();
			
			stopwatches[2].start();
			JNINativeMemory.readInt(address);
			stopwatches[2].stopAndAccumulate();
			
			stopwatches[3].start();
//			for (int j = 0; j < data.length; j++) {
//				JNINativeMemory.writeByte(address + j, data[j]);
//			}
			JNINativeMemory.write(address, data, 0, data.length);
			stopwatches[3].stopAndAccumulate();
			
			stopwatches[4].start();
//			for (int j = 0; j < data.length; j++) {
//				data[j] = JNINativeMemory.readByte(address + j);
//			}
			JNINativeMemory.read(address, data, 0, data.length);
			stopwatches[4].stopAndAccumulate();
			
			stopwatches[5].start();
			JNINativeMemory.free(address);
			stopwatches[5].stopAndAccumulate();
			
			// sleep to kill performance gains by continuous looping
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		m_table.set(p_rowName, "create", stopwatches[0].getTimeStr());
		m_table.set(p_rowName, "putInt", stopwatches[1].getTimeStr());
		m_table.set(p_rowName, "getInt", stopwatches[2].getTimeStr());
		m_table.set(p_rowName, "putByteArray", stopwatches[3].getTimeStr());
		m_table.set(p_rowName, "getByteArray", stopwatches[4].getTimeStr());
		m_table.set(p_rowName, "remove", stopwatches[5].getTimeStr());
	}
}
