package de.uniduesseldorf.dxram.core.chunk;

public interface DataStructure 
{
	public long getID();
	
	public void write(final long p_startAddress, final DataStructureWriter p_writer);
	
	public void read(final long p_startAddress, final DataStructureReader p_reader);
	
	public int sizeof();
	
	public boolean hasDynamicSize();
}
