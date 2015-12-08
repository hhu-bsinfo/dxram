package de.uniduesseldorf.dxram.core.chunk;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.DXRAMCoreInterface;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;

public class DXRAMCore implements DXRAMCoreInterface, MessageReceiver, ConnectionLostListener
{
	// Constants
	private final Logger LOGGER = Logger.getLogger(DXRAMCore.class);
	
	// configuration values
	private final boolean M_STATISTICS_ENABLED;
	private final boolean M_LOG_ACTIVE;
	private final long M_SECONDARY_LOG_SIZE;
	private final int M_REPLICATION_FACTOR;
	
	private MemoryManager m_memoryManager;
	private LookupInterface m_lookup;
	
	public DXRAMCore(final boolean p_statisticsEnabled, final boolean p_logActive, final long p_secondaryLogSize, final int p_replicationFactor)
	{
		M_STATISTICS_ENABLED = p_statisticsEnabled;
		M_LOG_ACTIVE = p_logActive;
		M_SECONDARY_LOG_SIZE = p_secondaryLogSize;
		M_REPLICATION_FACTOR = p_replicationFactor;
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
		long chunkID;

		if (M_STATISTICS_ENABLED)
			Operation.CREATE.enter();

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			m_memoryManager.lockManage();
			chunkID = m_memoryManager.create(p_size);
			if (chunkID != ChunkID.INVALID_ID) {
				initBackupRange(ChunkID.getLocalID(chunkID), p_size);
				m_memoryManager.unlockManage();
			} else {
				m_memoryManager.unlockManage();
			}
		}

		if (M_STATISTICS_ENABLED)
			Operation.CREATE.leave();

		return chunkID;
	}

	@Override
	public long[] create(int[] p_sizes) {
		long[] chunkIDs = null;

		if (M_STATISTICS_ENABLED) {
			Operation.MULTI_CREATE.enter();
		}
			
		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			chunkIDs = new long[p_sizes.length];

			m_memoryManager.lockManage();
			// keep first loop tight and execute everything
			// that we don't have to lock outside of this section
			for (int i = 0; i < p_sizes.length; i++) {
				chunkIDs[i] = m_memoryManager.create(p_sizes[i]);		
			}
			m_memoryManager.unlockManage();
			
			for (int i = 0; i < p_sizes.length; i++) {
				initBackupRange(ChunkID.getLocalID(chunkIDs[i]), p_sizes[i]);
			}
		}

		if (M_STATISTICS_ENABLED) {
			Operation.MULTI_CREATE.leave();
		}
		
		return chunkIDs;
	}

	@Override
	public int get(DataStructure p_dataStructure) {
		int ret = 0;
		short primaryPeer;
		GetRequest request;
		GetResponse response;
		Locations locations;

		if (M_STATISTICS_ENABLED)
			Operation.GET.enter();

		ChunkID.check(p_dataStructure.getID());

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			m_memoryManager.lockAccess();
			if (m_memoryManager.isResponsible(p_dataStructure.getID())) {
				// local get
				if (m_memoryManager.get(p_dataStructure))
					ret = 1;
				m_memoryManager.unlockAccess();
			} else {
				m_memoryManager.unlockAccess();

				while (null == ret) {
					locations = m_lookup.get(p_chunkID);
					if (locations == null) {
						break;
					}

					primaryPeer = locations.getPrimaryPeer();

					if (primaryPeer == m_nodeID) {
						// Local get
						int size = -1;
						int bytesRead = -1;

						m_memoryManager.lockAccess();
						size = m_memoryManager.getSize(p_chunkID);
						ret = new Chunk(p_chunkID, size);
						bytesRead = m_memoryManager.get(p_chunkID, ret.getData().array(), 0, size);
						m_memoryManager.unlockAccess();
					} else {
						// Remote get
						request = new GetRequest(primaryPeer, p_chunkID);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							if (LOG_ACTIVE) {
								// TODO: Start Recovery
							}
							continue;
						}
						response = request.getResponse(GetResponse.class);
						if (response != null) {
							ret = response.getChunk();
						}
					}
				}
			}
		}

		if (M_STATISTICS_ENABLED)
			Operation.GET.leave();

		return ret;
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
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_localID
	 *            the current LocalID
	 * @param p_size
	 *            the size of the new created chunk
	 * @param p_version
	 *            the version of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void initBackupRange(final long p_localID, final int p_size) throws LookupException {
		if (M_LOG_ACTIVE) {
			m_rangeSize += p_size + m_log.getHeaderSize(m_nodeID, p_localID, p_size, p_version);
			if (p_localID == 1 && p_version == 0) {
				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
				determineBackupPeers(0);
				m_lookup.initRange((long) m_nodeID << 48, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange((long) m_nodeID << 48, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			} else if (m_rangeSize > SECONDARY_LOG_SIZE / 2) {
				determineBackupPeers(p_localID);
				m_lookup.initRange(((long) m_nodeID << 48) + p_localID, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) m_nodeID << 48) + p_localID, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			}
		} else if (p_localID == 1 && p_version == 0) {
			m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL, new Locations(m_nodeID, new short[] {-1, -1, -1}, null));
		}
	}
}
