package de.hhu.bsinfo.dxram.test.nothaas;

import java.nio.ByteBuffer;

public class Sandbox {

	public static void main(String[] args)
	{
		ByteBuffer buffer = ByteBuffer.allocate(100);
		for (int i = 0; i < buffer.capacity(); i++)
		{
			buffer.put((byte) 5);
		}
		
		System.out.println("done");
	}
}
