package de.hhu.bsinfo.dxram.run.test.nothaas;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

public class NativeMemoryTest extends Main {

	public static final Pair<String, String> ARG_JNI_PATH = new Pair<String, String>("jniPath", "UNKNOWN");
	
	public static void main(final String[] args) {
		Main main = new NativeMemoryTest();
		main.run(args);
	}
	
	public NativeMemoryTest()
	{

	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_JNI_PATH);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		final String jniPath = p_arguments.getArgument(ARG_JNI_PATH);
		
		System.out.println("Loading jni file " + jniPath);
		JNINativeMemory.load(jniPath);
		
		long addr = JNINativeMemory.alloc(100);
		
		JNINativeMemory.set(addr, (byte) 0xAA, 100);
		
		byte[] array = new byte[20];
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) i;
		}
		JNINativeMemory.write(addr + 5, array, 5, array.length - 5);
		JNINativeMemory.read(addr, array, 0, array.length);
		
		for (int i = 0; i < array.length; i++) {
			System.out.print(Integer.toHexString(array[i]) + " ");
		}
		System.out.println();
		
		JNINativeMemory.dump(addr, 112, "dump.mem");
		
		JNINativeMemory.free(addr);

		return 0;
	}

}
