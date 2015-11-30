package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxram.core.chunk.Chunk;

public interface StorageInterface 
{
	public Chunk create(final int p_size);
	
	public Chunk[] create(final int[] p_sizes);
	
	public Chunk get(final long p_handle);
	
	public Chunk[] get(final long[] p_handles);
	
	public void put(final long p_handle, Chunk p_data);
	
	public void put(final long p_handle, Chunk[] p_data);
	
	public void remove(final long p_handle);
	
	public void remove(final long[] p_handles);
}
