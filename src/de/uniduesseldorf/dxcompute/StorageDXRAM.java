package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

import de.uniduesseldorf.dxcompute.data.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxcompute.data.DataStructure;

public class StorageDXRAM implements StorageDelegate
{
	public StorageDXRAM() throws DXRAMException
	{
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
	}
	
	@Override
	public long create(int p_size) 
	{		
		try {
			Chunk chunk = Core.createNewChunk(p_size);
			return chunk.getChunkID();
		} catch (DXRAMException e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public long[] create(int[] p_sizes) {
		try {
			Chunk[] chunks = Core.createNewChunks(p_sizes);
			long[] chunkIDs = new long[chunks.length];
			for (int i = 0; i < chunkIDs.length; i++)
				chunkIDs[i] = chunks[i].getChunkID();
			
			return chunkIDs;
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public int get(DataStructure p_dataStructure) {
		try {
			Chunk chunk = Core.get(p_dataStructure.getID());
			if (chunk == null)
				return 0;
			p_dataStructure.write(new ByteBufferDataStructureReaderWriter(chunk.getData()));
			return 1;
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int get(DataStructure[] p_dataStructures) {
		long[] ids = new long[p_dataStructures.length];
		for (int i = 0; i < ids.length; i++)
			ids[i] = p_dataStructures[i].getID();
		
		try {
			Chunk[] chunks = Core.get(ids);
			if (chunks == null)
				return 0;
			
			// ordering not guaranteed, we have to do this =(
			int received = 0;
			for (int i = 0; i < chunks.length; i++)
			{
				for (int j = 0; j < p_dataStructures.length; j++)
				{
					if (chunks[i].getChunkID() == p_dataStructures[j].getID())
					{
						received++;
						p_dataStructures[j].write(new ByteBufferDataStructureReaderWriter(chunks[i].getData()));
						break;
					}
				}
			}
			
			return received;
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int put(DataStructure p_dataStrucutre) {
		Chunk chunk = new Chunk(p_dataStrucutre.getID(), p_dataStrucutre.sizeof());
		
		p_dataStrucutre.read(new ByteBufferDataStructureReaderWriter(chunk.getData()));
		try {
			Core.put(chunk);
			return 1;
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int put(DataStructure[] p_dataStructure) {
		Chunk[] chunks = new Chunk[p_dataStructure.length];
		
		for (int i = 0; i < chunks.length; i++)
		{
			chunks[i] = new Chunk(p_dataStructure[i].getID(), p_dataStructure[i].sizeof());
			p_dataStructure[i].read(new ByteBufferDataStructureReaderWriter(chunks[i].getData()));
		}
		
		try {
			Core.put(chunks);
			return chunks.length;
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public int remove(DataStructure p_dataStructure) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public int remove(DataStructure[] p_dataStructures) {
		throw new RuntimeException("Not implemented");
	}

}
