package de.uniduesseldorf.dxcompute;

public interface DataStructure 
{
	public long getID();
	
	public void write(final DataStructureWriter p_writer);
	
	public void read(final DataStructureReader p_reader);
	
	public int sizeof();
}
