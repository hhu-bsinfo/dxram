package de.uniduesseldorf.dxram.core.api;

import de.uniduesseldorf.dxram.core.chunk.DataStructure;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public interface DXRAMCoreInterface 
{
	public boolean initialize() throws DXRAMException;
	
	public boolean shutdown() throws DXRAMException;
	
	public long create(final int p_size) throws DXRAMException;
	
	public long[] create(final int[] p_sizes) throws DXRAMException;
	
	public int get(final DataStructure p_dataStructure) throws DXRAMException;
	
	public int get(final DataStructure[] p_dataStructures) throws DXRAMException;
	
	public int put(final DataStructure p_dataStrucutre) throws DXRAMException;
	
	public int put(final DataStructure[] p_dataStructure) throws DXRAMException;
	
	public int remove(final DataStructure p_dataStructure) throws DXRAMException;
	
	public int remove(final DataStructure[] p_dataStructures) throws DXRAMException;
}
