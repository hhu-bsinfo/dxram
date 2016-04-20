
package de.hhu.bsinfo.dxram.backup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRangeWithBackupPeers;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Component for managing backup ranges.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class BackupComponent extends AbstractDXRAMComponent {

	private boolean m_backupActive;
	private String m_backupDirectory;
	private long m_backupRangeSize;
	private long m_rangeSize;
	private boolean m_firstRangeInitialized;
	private short m_replicationFactor;

	private short m_nodeID;

	private BackupRange m_currentBackupRange;
	private ArrayList<BackupRange> m_ownBackupRanges;

	private BackupRange m_currentMigrationBackupRange;
	private ArrayList<BackupRange> m_migrationBackupRanges;
	// ChunkID -> migration backup range
	private MigrationBackupTree m_migrationsTree;

	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	private LogComponent m_log;

	/**
	 * Creates the backup component
	 * @param p_priorityInit
	 *            the initialization priority
	 * @param p_priorityShutdown
	 *            the shutdown priority
	 */
	public BackupComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Returns whether backup is enabled or not
	 * @return whether backup is enabled or not
	 */
	public boolean isActive() {
		return m_backupActive;
	}

	/**
	 * Return the path to all logs
	 * @return the backup directory
	 */
	public String getBackupDirectory() {
		return m_backupDirectory;
	}

	/**
	 * Registers peer in superpeer overlay
	 */
	public void registerPeer() {
		m_lookup.initRange(0, new LookupRangeWithBackupPeers(m_nodeID, null, null));
	}

	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_chunkID
	 *            the current ChunkID
	 * @param p_size
	 *            the size of the new created chunk
	 */
	public void initBackupRange(final long p_chunkID, final int p_size) {
		final int size;
		final long localID = ChunkID.getLocalID(p_chunkID);

		if (m_backupActive) {
			size = p_size + m_log.getAproxHeaderSize(m_nodeID, localID, p_size);
			if (!m_firstRangeInitialized && localID == 1) {
				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
				// This is the first put and p_localID is not reused
				determineBackupPeers(0);
				m_lookup.initRange((long) m_nodeID << 48,
						new LookupRangeWithBackupPeers(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange((long) m_nodeID << 48, m_currentBackupRange.getBackupPeers());
				m_rangeSize = size;
				m_firstRangeInitialized = true;
			} else if (m_rangeSize + size > m_backupRangeSize) {
				determineBackupPeers(localID);
				m_lookup.initRange(((long) m_nodeID << 48) + localID,
						new LookupRangeWithBackupPeers(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) m_nodeID << 48) + localID, m_currentBackupRange.getBackupPeers());
				m_rangeSize = size;
			} else {
				m_rangeSize += size;
			}
		} else if (!m_firstRangeInitialized) {
			m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL,
					new LookupRangeWithBackupPeers(m_nodeID, new short[] {-1, -1, -1}, null));
			m_firstRangeInitialized = true;
		}
	}

	/**
	 * Returns the corresponding backup peers
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the backup peers
	 */
	public short[] getBackupPeersForLocalChunks(final long p_chunkID) {
		short[] ret = null;

		if (ChunkID.getCreatorID(p_chunkID) == m_nodeID) {
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

		if (ChunkID.getCreatorID(p_chunkID) == m_nodeID) {
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

	/**
	 * Initializes a new migration backup range
	 */
	public void initNewMigrationBackupRange() {
		determineBackupPeers(-1);
		m_migrationsTree.initNewBackupRange();

		m_lookup.initRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(),
				new LookupRangeWithBackupPeers(m_nodeID,
						m_currentMigrationBackupRange.getBackupPeers(), null));
		m_log.initBackupRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(),
				m_currentMigrationBackupRange.getBackupPeers());
	}

	/**
	 * Returns the backup peers for current migration backup range
	 * @return the backup peers for current migration backup range
	 */
	public short[] getCurrentMigrationBackupPeers() {
		return m_currentMigrationBackupRange.getBackupPeers();
	}

	/**
	 * Puts a migrated chunk into the migration tree
	 * @param p_chunk
	 *            the migrated chunk
	 * @return the RangeID of the migration backup range the chunk was put in
	 */
	public byte addMigratedChunk(final Chunk p_chunk) {
		final byte rangeID = (byte) m_currentMigrationBackupRange.getRangeID();

		m_migrationsTree.putObject(p_chunk.getID(), rangeID, p_chunk.getDataSize());

		return rangeID;
	}

	/**
	 * Checks if given log entry fits in current migration backup range
	 * @param p_size
	 *            the range size
	 * @param p_logEntrySize
	 *            the log entry size
	 * @return whether the entry and range fits in backup range
	 */
	public boolean fitsInCurrentMigrationBackupRange(final long p_size, final int p_logEntrySize) {
		return m_migrationsTree.fits(p_size + p_logEntrySize) && (m_migrationsTree.size() != 0 || p_size > 0);
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(BackupConfigurationValues.Component.BACKUP_ACTIVE);
		p_settings.setDefaultValue(BackupConfigurationValues.Component.BACKUP_DIRECTORY);
		p_settings.setDefaultValue(BackupConfigurationValues.Component.BACKUP_RANGE_SIZE);
		p_settings.setDefaultValue(BackupConfigurationValues.Component.REPLICATION_FACTOR);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_backupActive = p_settings.getValue(BackupConfigurationValues.Component.BACKUP_ACTIVE);
		m_backupDirectory = p_settings.getValue(BackupConfigurationValues.Component.BACKUP_DIRECTORY);
		m_backupRangeSize = p_settings.getValue(BackupConfigurationValues.Component.BACKUP_RANGE_SIZE);
		m_replicationFactor = p_settings.getValue(BackupConfigurationValues.Component.REPLICATION_FACTOR);
		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);
		m_lookup = getDependentComponent(LookupComponent.class);
		m_log = getDependentComponent(LogComponent.class);

		m_nodeID = m_boot.getNodeID();
		if (m_backupActive && m_boot.getNodeRole().equals(NodeRole.PEER)) {
			m_ownBackupRanges = new ArrayList<BackupRange>();
			m_migrationBackupRanges = new ArrayList<BackupRange>();
			m_migrationsTree = new MigrationBackupTree((short) 10, m_backupRangeSize);
			m_currentBackupRange = null;
			m_currentMigrationBackupRange = new BackupRange(-1, null);
			m_rangeSize = 0;

			getDependentComponent(NetworkComponent.class).registerMessageType(LogMessages.TYPE, LogMessages.SUBTYPE_INIT_REQUEST, InitRequest.class);
			getDependentComponent(NetworkComponent.class).registerMessageType(LogMessages.TYPE, LogMessages.SUBTYPE_INIT_RESPONSE, InitResponse.class);
		}
		m_firstRangeInitialized = false;

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

	/**
	 * Determines backup peers
	 * @param p_localID
	 *            the current LocalID
	 */
	private void determineBackupPeers(final long p_localID) {
		boolean ready = false;
		boolean insufficientPeers = false;
		short index = 0;
		short[] oldBackupPeers = null;
		short[] newBackupPeers = null;
		short numberOfPeers = 0;

		List<Short> peers = null;
		// Get all other online peers
		peers = m_boot.getIDsOfOnlinePeers();
		numberOfPeers = (short) peers.size();

		/*-peers = new ArrayList<Short>();
		peers.add((short) -15999);
		peers.add((short) 320);
		peers.add((short) -15615);
		numberOfPeers = 3;*/

		if (3 > numberOfPeers) {
			m_logger.warn(BackupComponent.class,
					"Less than three peers for backup available. Replication will be incomplete!");

			newBackupPeers = new short[numberOfPeers];
			Arrays.fill(newBackupPeers, (short) -1);

			insufficientPeers = true;
		} else if (6 > numberOfPeers) {
			m_logger.warn(BackupComponent.class, "Less than six peers for backup available. Some peers may store more"
					+ " than one backup range of a node!");

			oldBackupPeers = new short[m_replicationFactor];
			Arrays.fill(oldBackupPeers, (short) -1);

			newBackupPeers = new short[m_replicationFactor];
			Arrays.fill(newBackupPeers, (short) -1);
		} else {
			if (null != m_currentBackupRange) {
				oldBackupPeers = new short[m_replicationFactor];
				for (int i = 0; i < m_replicationFactor; i++) {
					if (p_localID > -1) {
						oldBackupPeers[i] = m_currentBackupRange.getBackupPeers()[i];
					} else {
						oldBackupPeers[i] = m_currentMigrationBackupRange.getBackupPeers()[i];
					}
				}
			} else {
				oldBackupPeers = new short[m_replicationFactor];
				Arrays.fill(oldBackupPeers, (short) -1);
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
							if (peers.get(index) == newBackupPeers[j]) {
								ready = false;
								break;
							}
						}
					}
					m_logger.info(BackupComponent.class, i + 1 + ". backup peer determined for new range "
							+ ChunkID.toHexString(((long) m_nodeID << 48) + p_localID) + ": " + NodeID.toHexString(peers.get(index)));
					newBackupPeers[i] = peers.get(index);
					ready = false;
				}
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
				} else {
					m_currentMigrationBackupRange =
							new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
				}
			} else {
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, null);
				} else {
					m_currentMigrationBackupRange =
							new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, null);
				}
			}
		} else {
			// Determine backup peers
			for (int i = 0; i < 3; i++) {
				while (!ready) {
					index = (short) (Math.random() * numberOfPeers);
					ready = true;
					for (int j = 0; j < i; j++) {
						if (peers.get(index) == oldBackupPeers[j] || peers.get(index) == newBackupPeers[j]) {
							ready = false;
							break;
						}
					}
				}
				m_logger.info(BackupComponent.class, i + 1 + ". backup peer determined for new range "
						+ ChunkID.toHexString(((long) m_nodeID << 48) + p_localID) + ": " + NodeID.toHexString(peers.get(index)));
				newBackupPeers[i] = peers.get(index);
				ready = false;
			}
			if (p_localID > -1) {
				m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
			} else {
				m_currentMigrationBackupRange =
						new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
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
