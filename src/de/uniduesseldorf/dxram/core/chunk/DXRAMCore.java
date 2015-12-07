package de.uniduesseldorf.dxram.core.chunk;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.DXRAMCoreInterface;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;

public class DXRAMCore implements DXRAMCoreInterface, MessageReceiver, ConnectionLostListener
{
	// Constants
	private final Logger LOGGER = Logger.getLogger(DXRAMCore.class);
	
	public DXRAMCore()
	{
		
	}
	
	@Override
	public boolean initialize() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean shutdown() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public long create(int p_size) 
	{
		Chunk ret = null;
		long chunkID;

		// TODO get configuration value on init if we should enable this
		// and have if blocks surrounding them
		Operation.CREATE.enter();

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			m_memoryManager.lockManage();
			chunkID = m_memoryManager.create(p_size);
			if (chunkID != -1) {
				int version = -1;

				version = m_memoryManager.getVersion(chunkID);
				initBackupRange(ChunkID.getLocalID(chunkID), p_size, version);
				m_memoryManager.unlockManage();

				ret = new Chunk(chunkID, p_size);
			} else {
				m_memoryManager.unlockManage();
			}
		}

		Operation.CREATE.leave();

		return ret;
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
	
	

	@Override
	public void triggerEvent(ConnectionLostEvent p_event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		// TODO Auto-generated method stub
		
	}
}
