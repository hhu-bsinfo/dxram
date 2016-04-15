
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.CacheTree;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayPeer;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlaySuperpeer;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Cache;

/**
 * Component for finding chunks in superpeer overlay.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class LookupComponent extends AbstractDXRAMComponent {

	private static final short ORDER = 10;

	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;

	private OverlaySuperpeer m_superpeer;
	private OverlayPeer m_peer;

	private boolean m_cachesEnabled;
	private long m_maxCacheSize = -1;
	private CacheTree m_chunkIDCacheTree;
	private Cache<Integer, Long> m_applicationIDCache;

	/**
	 * Creates the lookup component
	 * @param p_priorityInit
	 *            the initialization priority
	 * @param p_priorityShutdown
	 *            the shutdown priority
	 */
	public LookupComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Get the corresponding LookupRange for the given ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the current location and the range borders
	 */
	public LookupRange getLookupRange(final long p_chunkID) {
		LookupRange ret = null;

		m_logger.trace(getClass(), "Entering get with: p_chunkID=" + ChunkID.toHexString(p_chunkID));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				// Read from cache
				ret = m_chunkIDCacheTree.getMetadata(p_chunkID);
				if (ret == null) {
					// Cache miss -> get LookupRange from superpeer
					ret = m_peer.getLookupRange(p_chunkID);

					// Add response to cache
					if (ret != null) {
						m_chunkIDCacheTree.cacheRange(
								((long) ChunkID.getCreatorID(p_chunkID) << 48) + ret.getRange()[0],
								((long) ChunkID.getCreatorID(p_chunkID) << 48) + ret.getRange()[1],
								ret.getPrimaryPeer());
					}
				}
			} else {
				ret = m_peer.getLookupRange(p_chunkID);
			}
		}

		m_logger.trace(getClass(), "Exiting get");
		return ret;
	}

	/**
	 * Remove the ChunkIDs from range after deletion of that chunks
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 */
	public void removeChunkIDs(final long[] p_chunkIDs) {

		m_logger.trace(getClass(), "Entering remove with: p_chunkIDs=" + p_chunkIDs);

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				invalidate(p_chunkIDs);
			}
			m_peer.removeChunkIDs(p_chunkIDs);
		}

		m_logger.trace(getClass(), "Exiting remove");
	}

	/**
	 * Insert a new name service entry
	 * @param p_id
	 *            the AID
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public void insertNameserviceEntry(final int p_id, final long p_chunkID) {

		// Insert ChunkID <-> ApplicationID mapping
		m_logger.trace(getClass(), "Entering insertID with: p_id=" + p_id + ", p_chunkID=" + ChunkID.toHexString(p_chunkID));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				m_applicationIDCache.put(p_id, p_chunkID);
			}

			m_peer.insertNameserviceEntry(p_id, p_chunkID);
		}

		m_logger.trace(getClass(), "Exiting insertID");
	}

	/**
	 * Get ChunkID for give AID
	 * @param p_id
	 *            the AID
	 * @param p_timeoutMs
	 *            Timeout for trying to get the entry (if it does not exist, yet).
	 *            set this to -1 for infinite loop if you know for sure, that the entry has to exist
	 * @return the corresponding ChunkID
	 */
	public long getChunkIDForNameserviceEntry(final int p_id, final int p_timeoutMs) {
		long ret = -1;

		// Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
		m_logger.trace(getClass(), "Entering getChunkID with: p_id=" + p_id);

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				// Read from application cache first
				final Long chunkID = m_applicationIDCache.get(p_id);

				if (null == chunkID) {
					// Cache miss -> ask superpeer
					m_logger.trace(getClass(), "value not cached for application cache: " + p_id);

					ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);

					// Cache response
					m_applicationIDCache.put(p_id, ret);
				} else {
					ret = chunkID.longValue();
				}
			} else {
				ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);
			}
		}

		m_logger.trace(getClass(), "Exiting getChunkID");

		return ret;
	}

	/**
	 * Get the number of entries in name service
	 * @return the number of name service entries
	 */
	public int getNameserviceEntryCount() {
		int ret = -1;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			ret = m_peer.getNameserviceEntryCount();
		}

		return ret;
	}

	/**
	 * Store migration of given ChunkID to a new location
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_nodeID
	 *            the new owner
	 */
	public void migrate(final long p_chunkID, final short p_nodeID) {

		m_logger.trace(getClass(), "Entering migrate with: p_chunkID=" + ChunkID.toHexString(p_chunkID)
				+ ", p_nodeID=" + NodeID.toHexString(p_nodeID));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				invalidate(p_chunkID);
			}

			m_peer.migrate(p_chunkID, p_nodeID);
		}

		m_logger.trace(getClass(), "Exiting migrate");
	}

	/**
	 * Store migration of a range of ChunkIDs to a new location
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 * @param p_nodeID
	 *            the new owner
	 */
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {

		m_logger.trace(getClass(), "Entering migrateRange with: p_startChunkID=" + ChunkID.toHexString(p_startCID)
				+ ", p_endChunkID=" + ChunkID.toHexString(p_endCID) + ", p_nodeID=" + NodeID.toHexString(p_nodeID));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			if (m_cachesEnabled) {
				invalidate(p_startCID, p_endCID);
			}

			m_peer.migrateRange(p_startCID, p_endCID, p_nodeID);
		}

		m_logger.trace(getClass(), "Exiting migrateRange");
	}

	/**
	 * Initialize a new backup range
	 * @param p_firstChunkIDOrRangeID
	 *            the RangeID or ChunkID of the first chunk in range
	 * @param p_primaryAndBackupPeers
	 *            the creator and all backup peers
	 */
	public void initRange(final long p_firstChunkIDOrRangeID,
			final LookupRangeWithBackupPeers p_primaryAndBackupPeers) {

		m_logger.trace(getClass(), "Entering initRange with: p_endChunkID="
				+ ChunkID.toHexString(p_firstChunkIDOrRangeID) + ", p_primaryAndBackupPeers="
				+ p_primaryAndBackupPeers);

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			m_peer.initRange(p_firstChunkIDOrRangeID, p_primaryAndBackupPeers);
		}

		m_logger.trace(getClass(), "Exiting initRange");
	}

	/**
	 * Get all backup ranges for given node
	 * @param p_nodeID
	 *            the NodeID
	 * @return all backup ranges for given node
	 */
	public BackupRange[] getAllBackupRanges(final short p_nodeID) {
		BackupRange[] ret = null;

		m_logger.trace(getClass(), "Entering getAllBackupRanges with: p_nodeID=" + NodeID.toHexString(p_nodeID));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			ret = m_peer.getAllBackupRanges(p_nodeID);
		}

		m_logger.trace(getClass(), "Exiting getAllBackupRanges");
		return ret;
	}

	/**
	 * Set restorer as new creator for recovered chunks
	 * @param p_owner
	 *            NodeID of the recovered peer
	 */
	public void setRestorerAfterRecovery(final short p_owner) {

		m_logger.trace(getClass(), "Entering updateAllAfterRecovery with: p_owner=" + NodeID.toHexString(p_owner));

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "Superpeer must not call this method!");
		} else {
			m_peer.setRestorerAfterRecovery(p_owner);
		}

		m_logger.trace(getClass(), "Exiting updateAllAfterRecovery");
	}

	/**
	 * Checks if all superpeers are offline
	 * @return if all superpeers are offline
	 */
	public boolean isResponsibleForBootstrapCleanup() {
		boolean ret = false;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			ret = m_superpeer.isLastSuperpeer();
		} else {
			ret = m_peer.allSuperpeersDown();
		}

		return ret;
	}

	/**
	 * Invalidates the cache entry for given ChunkIDs
	 * @param p_chunkIDs
	 *            the IDs
	 */
	public void invalidate(final long... p_chunkIDs) {
		for (long chunkID : p_chunkIDs) {
			assert chunkID != ChunkID.INVALID_ID;
			m_chunkIDCacheTree.invalidateChunkID(chunkID);
		}
	}

	/**
	 * Invalidates the cache entry for given ChunkID range
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 */
	public void invalidate(final long p_startCID, final long p_endCID) {
		long iter = p_startCID;
		while (iter <= p_endCID) {
			invalidate(iter++);
		}
	}

	/**
	 * Clear the cache
	 */
	@SuppressWarnings("unused")
	private void clear() {
		m_chunkIDCacheTree = new CacheTree(m_maxCacheSize, ORDER);
		m_applicationIDCache.clear();
	}

	// --------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(LookupConfigurationValues.Component.CACHES_ENABLED);
		p_settings.setDefaultValue(LookupConfigurationValues.Component.CACHE_ENTRIES);
		p_settings.setDefaultValue(LookupConfigurationValues.Component.NAMESERVICE_CACHE_ENTRIES);
		p_settings.setDefaultValue(LookupConfigurationValues.Component.CACHE_TTL);
		p_settings.setDefaultValue(LookupConfigurationValues.Component.PING_INTERVAL);
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);

		m_cachesEnabled = p_settings.getValue(LookupConfigurationValues.Component.CACHES_ENABLED);
		if (m_cachesEnabled) {
			m_maxCacheSize = p_settings.getValue(LookupConfigurationValues.Component.CACHE_ENTRIES);

			m_chunkIDCacheTree = new CacheTree(m_maxCacheSize, ORDER);

			m_applicationIDCache = new Cache<Integer, Long>(
					p_settings.getValue(LookupConfigurationValues.Component.NAMESERVICE_CACHE_ENTRIES));
			// m_aidCache.enableTTL();
		}

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_superpeer = new OverlaySuperpeer(m_boot.getNodeID(), m_boot.getNodeIDBootstrap(),
					m_boot.getNumberOfAvailableSuperpeers(),
					p_settings.getValue(LookupConfigurationValues.Component.PING_INTERVAL), m_boot, m_logger,
					getDependentComponent(NetworkComponent.class), getDependentComponent(EventComponent.class));
		} else {
			m_peer = new OverlayPeer(m_boot.getNodeID(), m_boot.getNodeIDBootstrap(),
					m_boot.getNumberOfAvailableSuperpeers(), m_boot, m_logger,
					getDependentComponent(NetworkComponent.class));
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		if (m_superpeer != null) {
			m_superpeer.shutdown();
		}

		if (m_cachesEnabled) {
			if (m_chunkIDCacheTree != null) {
				m_chunkIDCacheTree.close();
				m_chunkIDCacheTree = null;
			}
			if (m_applicationIDCache != null) {
				m_applicationIDCache.clear();
				m_applicationIDCache = null;
			}
		}

		return true;
	}

}
