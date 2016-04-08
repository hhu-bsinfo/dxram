package de.hhu.bsinfo.utils.run;

import java.nio.ByteBuffer;
import java.util.Random;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Test and verify if the native memory implementation is working correctly 
 * (simple tests and endianess test, only).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class NativeMemoryTest extends AbstractMain {

	public static final Argument ARG_JNI_PATH = new Argument("jniPath", null, false, "Path to JNI file with native memory implementation");
	
	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		AbstractMain main = new NativeMemoryTest();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	public NativeMemoryTest()
	{
		super("Testing JNI native memory implementation, especially if endianness is working correctly");
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_JNI_PATH);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		final String jniPath = p_arguments.getArgument(ARG_JNI_PATH).getValue(String.class);
		
		System.out.println("Loading jni file " + jniPath);
		JNINativeMemory.load(jniPath);
		
		final int size = 32;
		long addr = JNINativeMemory.alloc(size);
		System.out.println("Allocating " + size + " bytes of memory.");
		if (addr == 0)
		{
			System.out.println("Allocation failed.");
			return -1;
		}
		
		System.out.println("Nulling memory...");
		JNINativeMemory.set(addr, (byte) 0, size);
		
		System.out.println("Test writing byte array...");
		
		byte[] array = new byte[(int) size];
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) i;
		}
		JNINativeMemory.write(addr, array, 0, array.length);
		JNINativeMemory.read(addr, array, 0, array.length);
		
		for (int i = 0; i < array.length; i++) {
			if (array[i] != i % 256)
			{
				System.out.println("ERROR: Verifying byte array failed: " + i);
				return -2;
			}
		}
		
		System.out.println("Writing single byte...");
		
		// pick a random address:
		Random rand = new Random();
		{
			long addrOffset = (long) (rand.nextFloat() * size);
			
			final byte v = (byte) (rand.nextFloat() * 0xFF);
			JNINativeMemory.writeByte(addr + addrOffset, v);
			byte v2 = JNINativeMemory.readByte(addr + addrOffset);
			if (v != v2)
			{
				System.out.println("ERROR: Verifying write byte failed: " + v + " != " + v2);
				return -3;
			}
		}

		{
			long addrOffset = (long) (rand.nextFloat() * size);
			
			final short v = (short) (rand.nextFloat() * 0xFFFF);
			JNINativeMemory.writeShort(addr + addrOffset, v);
			short v2 = JNINativeMemory.readShort(addr + addrOffset);
			if (v != v2)
			{
				System.out.println("ERROR: Verifying write short failed: " + 
						Integer.toHexString(v) + " != " + Integer.toHexString(v2));
				return -4;
			}
		}
		
		{
			long addrOffset = (long) (rand.nextFloat() * size);
			
			final int v = (short) (rand.nextFloat() * 0xFFFFFFFF);
			JNINativeMemory.writeInt(addr + addrOffset, v);
			int v2 = JNINativeMemory.readInt(addr + addrOffset);
			if (v != v2)
			{
				System.out.println("ERROR: Verifying write int failed: " + 
						Integer.toHexString(v) + " != " + Integer.toHexString(v2));
				return -5;
			}
		}
		
		{
			long addrOffset = (long) (rand.nextFloat() * size);
			
			final long v = (short) (rand.nextFloat() * 0xFFFFFFFFFFFFFFFFL);
			JNINativeMemory.writeLong(addr + addrOffset, v);
			long v2 = JNINativeMemory.readLong(addr + addrOffset);
			if (v != v2)
			{
				System.out.println("ERROR: Verifying write long failed: " + 
						Long.toHexString(v) + " != " + Long.toHexString(v2));
				return -6;
			}
		}

		System.out.println("Endianess test...");
		
		for (int i = 0; i < 5; i++) {
			JNINativeMemory.writeInt(addr + i * Integer.BYTES, i + 1);
		}

		ByteBuffer buffer = ByteBuffer.allocate(5 * Integer.BYTES);
		JNINativeMemory.read(addr, buffer.array(), 0, buffer.capacity());
		
		for (int i = 0; i < 5; i++)
		{
			int val = buffer.getInt();
			if (val != i + 1)
			{
				System.out.println("ERROR: Endianess test: " + val + " != " + (i + 1));
				return -7;
			}
		}
		
		{
			long[] testArray = new long[5];
			long[] arrayRead = new long[5];
			for (int i = 0; i < 5; i++) {
				testArray[i] = 1;
			}
			
			JNINativeMemory.writeLongs(addr, testArray, 0, testArray.length);
			JNINativeMemory.readLongs(addr, arrayRead, 0, arrayRead.length);
			
			for (int i = 0; i < 5; i++) {
				if (testArray[i] != arrayRead[i]) {
					System.out.println("ERROR: Endianess test arrays: " + arrayRead[i] + " != " + testArray[i]);
					return -8;
				}
			}
		}
		System.out.println("Test readValue/writeValue...");
		{
			final long value = 0x1122334455667788L;
			for (int i = 0; i < 8; i++)
			{
				JNINativeMemory.writeValue(addr, value, i + 1);
				long testValue = JNINativeMemory.readValue(addr, i + 1);
				long testValue2 = (value & (0xFFFFFFFFFFFFFFFFL >>> (8 * (7 - i))));
				
				if (testValue != testValue2)
				{
					System.out.println("ERROR: readValue/writeValue test(" + i + "): " + Long.toHexString(testValue) + " != " + Long.toHexString(testValue2));
					return -9;
				}
			}
		}
		
		JNINativeMemory.free(addr);
		
		System.out.println("Done.");

		return 0;
	}

}
