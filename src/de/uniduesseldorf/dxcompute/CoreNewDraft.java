package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxram.core.mem.DataStructure;

public interface CoreNewDraft 
{	
	public long create(final int p_size);
	
	public long[] create(final int[] p_sizes);
	
	public int get(final DataStructure p_dataStructure);
	
	public int get(final long p_chunkID, final byte[] p_buffer, final int p_bufferOffset, final int p_length);
	
	public int get(final DataStructure[] p_dataStructures);
	
	public int put(final DataStructure p_dataStrucutre);
	
	public int put(final long p_chunkID, final byte[] p_buffer, final int p_bufferOffset, final int p_length);
	
	public int put(final DataStructure[] p_dataStructure);
	
	public int remove(final DataStructure p_dataStructure);
	
	public int remove(final long p_chunkID);
	
	public int remove(final DataStructure[] p_dataStructures);
	
	public int remove(final long[] p_chunkIDs);
}
