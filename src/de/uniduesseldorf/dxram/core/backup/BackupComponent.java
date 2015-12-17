package de.uniduesseldorf.dxram.core.backup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.config.DXRAMConfigurationConstants;
import de.uniduesseldorf.dxram.core.log.LogComponent;
import de.uniduesseldorf.dxram.core.lookup.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.lookup.LookupException;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.utils.config.Configuration;

public class BackupComponent extends DXRAMComponent {
	private static final Logger LOGGER = Logger.getLogger(BackupComponent.class);
	
	public static final String COMPONENT_IDENTIFIER = "Lookup";
	
	private LookupComponent m_lookup = null;
	private LogComponent m_logging = null;
	
	private boolean m_logActive = false;
	private long m_secondaryLogSize = -1;
	private int m_replicationFactor = -1;
	
	// ChunkID -> migration backup range
	private MigratedBackupsTree m_migrationsTree = null;
	
	private long m_rangeSize = -1;
	private boolean m_firstRangeInitialized = false;
	
	private BackupRange m_currentBackupRange = null;
	private ArrayList<BackupRange> m_ownBackupRanges = null;

	private BackupRange m_currentMigrationBackupRange = null;
	private ArrayList<BackupRange> m_migrationBackupRanges = null;
	
	public BackupComponent(int p_priorityInit, int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);
	}
	
	@Override
	protected boolean initComponent(Configuration p_configuration) {
		m_lookup = getDependantComponent(LookupComponent.COMPONENT_IDENTIFIER);
		m_logging = getDependantComponent(LogComponent.COMPONENT_IDENTIFIER);
		
		m_logActive = p_configuration.getBooleanValue(DXRAMConfigurationConstants.LOG_ACTIVE);
		m_secondaryLogSize = p_configuration.getLongValue(DXRAMConfigurationConstants.SECONDARY_LOG_SIZE);
		m_replicationFactor = p_configuration.getIntValue(DXRAMConfigurationConstants.REPLICATION_FACTOR);
		
		m_migrationsTree = new MigratedBackupsTree((short) 10, m_secondaryLogSize);
		
		m_rangeSize = 0;
		m_firstRangeInitialized = false;
		
		m_currentBackupRange = null;
		m_ownBackupRanges = new ArrayList<BackupRange>();
		
		m_currentMigrationBackupRange = new BackupRange(-1, null);
		m_migrationBackupRanges = new ArrayList<BackupRange>();
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_lookup = null;
		m_logging = null;
		
		m_migrationsTree = null;
		
		m_currentBackupRange = null;
		m_ownBackupRanges = null;
		
		m_currentMigrationBackupRange = null;
		m_migrationBackupRanges = null;
		
		return true;
	}
	
	/**
	 * Registers peer in superpeer overlay
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	public void registerPeer() {
		m_lookup.initRange(0, new Locations(getSystemData().getNodeID(), null, null));
	}
	
	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_localID
	 *            the current LocalID
	 * @param p_size
	 *            the size of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	public void initBackupRange(final long p_localID, final int p_size) {
		short nodeID = getSystemData().getNodeID();
		
		if (m_logActive) {
			m_rangeSize += p_size + m_logging.getAproxHeaderSize(nodeID, p_localID, p_size);
			if (!m_firstRangeInitialized && p_localID == 1) {
				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
				// This is the first put and p_localID is not reused
				determineBackupPeers(0);
				m_lookup.initRange((long) nodeID << 48, new Locations(nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_logging.initBackupRange((long) nodeID << 48, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
				m_firstRangeInitialized = true;
			} else if (m_rangeSize > m_secondaryLogSize / 2) {
				determineBackupPeers(p_localID);
				m_lookup.initRange(((long) nodeID << 48) + p_localID, new Locations(nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_logging.initBackupRange(((long) nodeID << 48) + p_localID, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			}
		} else if (!m_firstRangeInitialized && p_localID == 1) {
			m_lookup.initRange(((long) nodeID << 48) + 0xFFFFFFFFFFFFL, new Locations(nodeID, new short[] {-1, -1, -1}, null));
			m_firstRangeInitialized = true;
		}
	}
	
	public byte getBackupRange(final long p_chunkID) {
		return m_migrationsTree.getBackupRange(p_chunkID);
	}
	
	/**
	 * Returns the corresponding backup peers
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the backup peers
	 */
	public short[] getBackupPeersForLocalChunks(final long p_chunkID) {
		short[] ret = null;

		if (ChunkID.getCreatorID(p_chunkID) == getSystemData().getNodeID()) {
			for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
				if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
					ret = m_ownBackupRanges.get(i).getBackupPeers();
					break;
				}
			}
		} else {
			ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeers();
		}

		return ret;
	}

	/**
	 * Returns the corresponding backup peers
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the backup peers
	 */
	public long getBackupPeersForLocalChunksAsLong(final long p_chunkID) {
		long ret = -1;

		if (ChunkID.getCreatorID(p_chunkID) == getSystemData().getNodeID()) {
			for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
				if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
					ret = m_ownBackupRanges.get(i).getBackupPeersAsLong();
					break;
				}
			}
		} else {
			ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeersAsLong();
		}

		return ret;
	}
	
	public void removeChunk(final long p_chunkID) {
		m_migrationsTree.removeObject(p_chunkID);
	}
	
	// ---------------------------------------------------------------------------------------------

	/**
	 * Determines backup peers
	 * @param p_localID
	 *            the current LocalID
	 */
	private void determineBackupPeers(final long p_localID) {
		boolean ready = false;
		boolean insufficientPeers = false;
		short index = 0;
		short peer;
		short[] oldBackupPeers = null;
		short[] newBackupPeers = null;
		short[] allPeers;
		short numberOfPeers = 0;
		List<String> peers = null;

		// Get all other online peers
		peers = getSystemData().zookeeperGetChildren("nodes/peers");

		allPeers = new short[peers.size() - 1];
		for (int i = 0; i < peers.size(); i++) {
			peer = Short.parseShort(peers.get(i));
			if (peer != getSystemData().getNodeID()) {
				allPeers[numberOfPeers++] = peer;
			}
		}

		if (3 > numberOfPeers) {
			LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");

			newBackupPeers = new short[numberOfPeers];
			Arrays.fill(newBackupPeers, (short) -1);

			insufficientPeers = true;
		} else if (6 > numberOfPeers) {
			LOGGER.warn("Less than six peers for backup available. Some peers may store more" + " than one backup range of a node!");

			oldBackupPeers = new short[m_replicationFactor];
			Arrays.fill(oldBackupPeers, (short) -1);

			newBackupPeers = new short[m_replicationFactor];
			Arrays.fill(newBackupPeers, (short) -1);
		} else if (null != m_currentBackupRange.getBackupPeers()) {
			oldBackupPeers = new short[m_replicationFactor];
			for (int i = 0; i < m_replicationFactor; i++) {
				if (p_localID > -1) {
					oldBackupPeers[i] = m_currentBackupRange.getBackupPeers()[i];
				} else {
					oldBackupPeers[i] = m_currentMigrationBackupRange.getBackupPeers()[i];
				}
			}

			newBackupPeers = new short[m_replicationFactor];
			Arrays.fill(newBackupPeers, (short) -1);
		}

		if (insufficientPeers) {
			if (numberOfPeers > 0) {
				// Determine backup peers
				for (int i = 0; i < numberOfPeers; i++) {
					while (!ready) {
						index = (short) (Math.random() * numberOfPeers);
						ready = true;
						for (int j = 0; j < i; j++) {
							if (allPeers[index] == newBackupPeers[j]) {
								ready = false;
								break;
							}
						}
					}
					System.out.println(i + 1 + ". backup peer: " + allPeers[index]);
					newBackupPeers[i] = allPeers[index];
					ready = false;
				}
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
				} else {
					m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
				}
			} else {
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, null);
				} else {
					m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, null);
				}
			}
		} else {
			// Determine backup peers
			for (int i = 0; i < 3; i++) {
				while (!ready) {
					index = (short) (Math.random() * numberOfPeers);
					ready = true;
					for (int j = 0; j < i; j++) {
						if (allPeers[index] == oldBackupPeers[j] || allPeers[index] == newBackupPeers[j]) {
							ready = false;
							break;
						}
					}
				}
				System.out.println(i + 1 + ". backup peer: " + allPeers[index]);
				newBackupPeers[i] = allPeers[index];
				ready = false;
			}
			if (p_localID > -1) {
				m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
			} else {
				m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
			}
		}

		if (numberOfPeers > 0) {
			if (p_localID > -1) {
				m_ownBackupRanges.add(m_currentBackupRange);
			} else {
				m_migrationBackupRanges.add(m_currentMigrationBackupRange);
			}
		}
	}
}
