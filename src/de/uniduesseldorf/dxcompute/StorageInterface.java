package de.uniduesseldorf.dxcompute;

public interface StorageInterface 
{
	public long create(final int p_size);
	
	public long[] create(final int[] p_sizes);
	
	public int get(final DataStructure p_dataStructure);
	
	public int get(final DataStructure[] p_dataStructures);
	
	public int put(final DataStructure p_dataStrucutre);
	
	public int put(final DataStructure[] p_dataStructure);
	
	public int remove(final DataStructure p_dataStructure);
	
	public int remove(final DataStructure[] p_dataStructures);
}
