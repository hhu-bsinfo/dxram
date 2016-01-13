package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxram.data.DataStructure;

public class StorageDummy implements StorageDelegate
{

	@Override
	public long create(int p_size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long[] create(int[] p_sizes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int get(DataStructure p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int get(DataStructure[] p_dataStructures) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int put(DataStructure p_dataStrucutre) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int put(DataStructure[] p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(DataStructure p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(DataStructure[] p_dataStructures) {
		// TODO Auto-generated method stub
		return 0;
	}

}
