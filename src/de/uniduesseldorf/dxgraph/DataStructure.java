package de.uniduesseldorf.dxgraph;

import java.nio.ByteBuffer;

public interface DataStructure 
{
	public void serialize(final ByteBuffer p_outputBuffer);
	
	public void deserialize(final ByteBuffer p_inputBuffer);
	
	public int sizeof();
}
