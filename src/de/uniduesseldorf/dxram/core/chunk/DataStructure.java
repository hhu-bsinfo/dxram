package de.uniduesseldorf.dxram.core.chunk;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public interface DataStructure 
{
	public long getID();
	
	public void write(final long p_startAddress, final DataStructureWriter p_writer) throws MemoryException;
	
	public void read(final long p_startAddress, final DataStructureReader p_reader) throws MemoryException;
	
	public int sizeof();
	
	public boolean hasDynamicSize();
}
