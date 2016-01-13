package de.hhu.bsinfo.dxram.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.lookup.Locations;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;

public class ChunkSortByOrigin {
	private ArrayList<DataStructure> m_localChunks = new ArrayList<DataStructure>();
	private Map<Short, ArrayList<DataStructure>> m_remoteChunksByPeers = new HashMap<Short, ArrayList<DataStructure>>();
	
	private ChunkSortByOrigin() {
		
	}
	
	public ArrayList<DataStructure> getLocalChunks() {
		return m_localChunks;
	}
	
	public Map<Short, ArrayList<DataStructure>> getRemoteChunksByPeers() {
		return m_remoteChunksByPeers;
	}
	
	public static ChunkSortByOrigin sort(final LookupComponent p_lookup, final MemoryManagerComponent p_memoryManager, final DataStructure... p_dataStructure) {
		ChunkSortByOrigin sorted = new ChunkSortByOrigin();
		
		// first loop: sort by local/remote chunks and backup peers
		p_memoryManager.lockAccess();
		for (DataStructure dataStructure : p_dataStructure) {
			if (p_memoryManager.exists(dataStructure.getID())) {
				sorted.m_localChunks.add(dataStructure);
			} else {
				// remote, figure out location and sort by peers
				Locations locations = p_lookup.get(dataStructure.getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = sorted.m_remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						sorted.m_remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(dataStructure);
				}
			}
		}
		p_memoryManager.unlockAccess();
		
		return sorted;
	}
}
