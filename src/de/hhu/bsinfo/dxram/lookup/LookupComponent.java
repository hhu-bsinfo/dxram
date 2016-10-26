
package de.hhu.bsinfo.dxram.lookup;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NameserviceCacheEntryUpdateEvent;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayPeer;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlaySuperpeer;
import de.hhu.bsinfo.dxram.lookup.overlay.cache.CacheTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.Cache;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Component for finding chunks in superpeer overlay.
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class LookupComponent extends AbstractDXRAMComponent implements EventListener<AbstractEvent> {

	private static final Logger LOGGER = LogManager.getFormatterLogger(LookupComponent.class.getSimpleName());

	private static final short ORDER = 10;

	// configuration values
	@Expose
	private boolean m_cachesEnabled = true;
	@Expose
	private long m_maxCacheEntries = 1000L;
	@Expose
	private int m_nameserviceCacheEntries = 1000000;
	@Expose
	private TimeUnit m_cacheTtl = new TimeUnit(1, TimeUnit.SEC);
	@Expose
	private int m_pingInterval = 1;
	@Expose
	private int m_maxBarriersPerSuperpeer = 1000;
	@Expose
	private int m_storageMaxNumEntries = 1000;
	@Expose
	private StorageUnit m_storageMaxSize = new StorageUnit(32, StorageUnit.MB);

	// dependent components
	private AbstractBootComponent m_boot;
	private EventComponent m_event;
	private NetworkComponent m_network;

	private OverlaySuperpeer m_superpeer;
	private OverlayPeer m_peer;

	private CacheTree m_chunkIDCacheTree;
	private Cache<Integer, Long> m_applicationIDCache;

	/**
	 * Creates the lookup component
	 */
	public LookupComponent() {
		super(DXRAMComponentOrder.Init.LOOKUP, DXRAMComponentOrder.Shutdown.LOOKUP);
	}

	/**
	 * Get the corresponding LookupRange for the given ChunkID
	 *
	 * @param p_chunkID the ChunkID
	 * @return the current location and the range borders
	 */
	public LookupRange getLookupRange(final long p_chunkID) {
		LookupRange ret = null;

		// #if LOGGER == TRACE
		LOGGER.trace("Entering get with: p_chunkID=0x%X", p_chunkID);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
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

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting get");
		// #endif /* LOGGER == TRACE */
		return ret;
	}

	/**
	 * Remove the ChunkIDs from range after deletion of that chunks
	 *
	 * @param p_chunkIDs the ChunkIDs
	 */
	public void removeChunkIDs(final long[] p_chunkIDs) {

		// #if LOGGER == TRACE
		LOGGER.trace("Entering remove with %d chunkIDs", p_chunkIDs.length);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			if (m_cachesEnabled) {
				invalidate(p_chunkIDs);
			}
			m_peer.removeChunkIDs(p_chunkIDs);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting remove");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Insert a new name service entry
	 *
	 * @param p_id      the AID
	 * @param p_chunkID the ChunkID
	 */
	public void insertNameserviceEntry(final int p_id, final long p_chunkID) {

		// Insert ChunkID <-> ApplicationID mapping
		// #if LOGGER == TRACE
		LOGGER.trace("Entering insertID with: p_id=%d, p_chunkID=0x%X", p_id, p_chunkID);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			if (m_cachesEnabled) {
				m_applicationIDCache.put(p_id, p_chunkID);
			}

			m_peer.insertNameserviceEntry(p_id, p_chunkID);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting insertID");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Get ChunkID for give AID
	 *
	 * @param p_id        the AID
	 * @param p_timeoutMs Timeout for trying to get the entry (if it does not exist, yet).
	 *                    set this to -1 for infinite loop if you know for sure, that the entry has to exist
	 * @return the corresponding ChunkID
	 */
	public long getChunkIDForNameserviceEntry(final int p_id, final int p_timeoutMs) {
		long ret = -1;

		// Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
		// #if LOGGER == TRACE
		LOGGER.trace("Entering getChunkID with: p_id=%d", p_id);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			if (m_cachesEnabled) {
				// Read from application cache first
				final Long chunkID = m_applicationIDCache.get(p_id);

				if (null == chunkID) {
					// Cache miss -> ask superpeer
					// #if LOGGER == TRACE
					LOGGER.trace("Value not cached for application cache: %d", p_id);
					// #endif /* LOGGER == TRACE */

					ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);

					// Cache response
					m_applicationIDCache.put(p_id, ret);
				} else {
					ret = chunkID;
				}
			} else {
				ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);
			}
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting getChunkID");
		// #endif /* LOGGER == TRACE */

		return ret;
	}

	/**
	 * Get the number of entries in name service
	 *
	 * @return the number of name service entries
	 */
	public int getNameserviceEntryCount() {
		int ret = -1;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			ret = m_peer.getNameserviceEntryCount();
		}

		return ret;
	}

	/**
	 * Get all available nameservice entries.
	 *
	 * @return List of nameservice entries or null on error.
	 */
	public ArrayList<Pair<Integer, Long>> getNameserviceEntries() {
		ArrayList<Pair<Integer, Long>> ret = null;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			ret = m_peer.getNameserviceEntries();
		}

		return ret;
	}

	/**
	 * Returns all known superpeers
	 *
	 * @return array with all superpeers
	 */
	public ArrayList<Short> getAllSuperpeers() {
		return m_peer.getAllSuperpeers();
	}

	/**
	 * Returns the responsible superpeer for given peer
	 *
	 * @return the responsible superpeer
	 */
	public short getResponsibleSuperpeer(final short p_nodeID) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
			return NodeID.INVALID_ID;
		} else {
			return m_peer.getResponsibleSuperpeer(p_nodeID);
		}
	}

	/**
	 * Get Lookup Tree from Superpeer
	 *
	 * @param p_nodeID the NodeID
	 * @return LookupTree from SuperPeerOverlay
	 * @note This method must be called by a superpeer
	 */
	public LookupTree superPeerGetLookUpTree(final short p_nodeID) {
		LookupTree ret = null;

		// #if LOGGER >= TRACE
		LOGGER.trace("Entering getSuperPeerLookUpTree with: p_nodeID=0x%X", p_nodeID);
		// #endif /* LOGGER >= TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			ret = m_superpeer.getLookupTree(p_nodeID);
		} else {
			LOGGER.error("Peer must not call this method!");
		}

		LOGGER.trace("Exiting getSuperPeerLookUpTree");
		return ret;
	}

	//

	/**
	 * Store migration of given ChunkID to a new location
	 *
	 * @param p_chunkID the ChunkID
	 * @param p_nodeID  the new owner
	 */
	public void migrate(final long p_chunkID, final short p_nodeID) {

		// #if LOGGER == TRACE
		LOGGER.trace("Entering migrate with: p_chunkID=0x%X, p_nodeID=0x%X", p_chunkID, p_nodeID);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			if (m_cachesEnabled) {
				invalidate(p_chunkID);
			}

			m_peer.migrate(p_chunkID, p_nodeID);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting migrate");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Store migration of a range of ChunkIDs to a new location
	 *
	 * @param p_startCID the first ChunkID
	 * @param p_endCID   the last ChunkID
	 * @param p_nodeID   the new owner
	 */
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {

		// #if LOGGER == TRACE
		LOGGER.trace("Entering migrateRange with: p_startChunkID=0x%X, p_endChunkID=0x%X, p_nodeID=0x%X",
				p_startCID, p_endCID, p_nodeID);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			if (m_cachesEnabled) {
				invalidate(p_startCID, p_endCID);
			}

			m_peer.migrateRange(p_startCID, p_endCID, p_nodeID);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting migrateRange");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Initialize a new backup range
	 *
	 * @param p_firstChunkIDOrRangeID the RangeID or ChunkID of the first chunk in range
	 * @param p_primaryAndBackupPeers the creator and all backup peers
	 */
	public void initRange(final long p_firstChunkIDOrRangeID,
			final LookupRangeWithBackupPeers p_primaryAndBackupPeers) {

		// #if LOGGER == TRACE
		LOGGER.trace("Entering initRange with: p_endChunkID=0x%X, p_primaryAndBackupPeers=%s",
				p_firstChunkIDOrRangeID, p_primaryAndBackupPeers);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			m_peer.initRange(p_firstChunkIDOrRangeID, p_primaryAndBackupPeers);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting initRange");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Get all backup ranges for given node
	 *
	 * @param p_nodeID the NodeID
	 * @return all backup ranges for given node
	 */
	public BackupRange[] getAllBackupRanges(final short p_nodeID) {
		BackupRange[] ret = null;

		// #if LOGGER == TRACE
		LOGGER.trace("Entering getAllBackupRanges with: p_nodeID=0x%X", p_nodeID);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			ret = m_peer.getAllBackupRanges(p_nodeID);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting getAllBackupRanges");
		// #endif /* LOGGER == TRACE */
		return ret;
	}

	/**
	 * Removes failed node from superpeer overlay
	 *
	 * @param p_failedNode NodeID of failed node
	 * @param p_role       NodeRole of failed node
	 * @return whether this superpeer is responsible for the failed node
	 */
	public boolean failureHandling(final short p_failedNode, final NodeRole p_role) {
		return m_superpeer.failureHandling(p_failedNode, p_role);
	}

	/**
	 * Set restorer as new creator for recovered chunks
	 *
	 * @param p_owner NodeID of the recovered peer
	 */
	public void setRestorerAfterRecovery(final short p_owner) {
		// #if LOGGER == TRACE
		LOGGER.trace("Entering updateAllAfterRecovery with: p_owner=0x%X", p_owner);
		// #endif /* LOGGER == TRACE */

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("Superpeer must not call this method!");
			// #endif /* LOGGER >= ERROR */
		} else {
			m_peer.setRestorerAfterRecovery(p_owner);
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting updateAllAfterRecovery");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Checks if all superpeers are offline
	 *
	 * @return if all superpeers are offline
	 */
	public boolean isResponsibleForBootstrapCleanup() {
		boolean ret;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			ret = m_superpeer.isLastSuperpeer();
		} else {
			ret = m_peer.allSuperpeersDown();
		}

		return ret;
	}

	/**
	 * Invalidates the cache entry for given ChunkIDs
	 *
	 * @param p_chunkIDs the IDs
	 */
	public void invalidate(final long... p_chunkIDs) {
		for (long chunkID : p_chunkIDs) {
			assert chunkID != ChunkID.INVALID_ID;
			m_chunkIDCacheTree.invalidateChunkID(chunkID);
		}
	}

	/**
	 * Invalidates the cache entry for given ChunkID range
	 *
	 * @param p_startCID the first ChunkID
	 * @param p_endCID   the last ChunkID
	 */
	public void invalidate(final long p_startCID, final long p_endCID) {
		long iter = p_startCID;
		while (iter <= p_endCID) {
			invalidate(iter++);
		}
	}

	/**
	 * Allocate a barrier for synchronizing multiple peers.
	 *
	 * @param p_size Size of the barrier, i.e. number of peers that have to sign on until the barrier gets released.
	 * @return Barrier identifier on success, -1 on failure.
	 */
	public int barrierAllocate(final int p_size) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed to allocate barriers");
			// #endif /* LOGGER >= ERROR */
			return BarrierID.INVALID_ID;
		}

		return m_peer.barrierAllocate(p_size);
	}

	/**
	 * Free an allocated barrier.
	 *
	 * @param p_barrierId Barrier to free.
	 * @return True if successful, false otherwise.
	 */
	public boolean barrierFree(final int p_barrierId) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed to free barriers");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return m_peer.barrierFree(p_barrierId);
	}

	/**
	 * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
	 *
	 * @param p_barrierId Id of an allocated barrier to change the size of.
	 * @param p_newSize   New size for the barrier.
	 * @return True if changing size was successful, false otherwise.
	 */
	public boolean barrierChangeSize(final int p_barrierId, final int p_newSize) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed to change barrier sizes");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return m_peer.barrierChangeSize(p_barrierId, p_newSize);
	}

	/**
	 * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
	 *
	 * @param p_barrierId  Id of the barrier to sign on to.
	 * @param p_customData Custom data to pass along with the sign on
	 * @return A pair consisting of the list of signed on peers and their custom data passed along
	 * with the sign ons, null on error
	 */
	public Pair<short[], long[]> barrierSignOn(final int p_barrierId, final long p_customData) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed to sign on to barriers");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return m_peer.barrierSignOn(p_barrierId, p_customData);
	}

	/**
	 * Get the status of a specific barrier.
	 *
	 * @param p_barrierId Id of the barrier.
	 * @return Array of currently signed on peers with the first index being the number of signed on peers or null on
	 * error.
	 */
	public short[] barrierGetStatus(final int p_barrierId) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed get status of barriers");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return m_peer.barrierGetStatus(p_barrierId);
	}

	/**
	 * Create a block of memory in the superpeer storage.
	 *
	 * @param p_storageId Storage id to use to identify the block.
	 * @param p_size      Size of the block to allocate
	 * @return True if successful, false on failure (no space, element count exceeded or id used).
	 */
	public boolean superpeerStorageCreate(final int p_storageId, final int p_size) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to his storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return m_peer.superpeerStorageCreate(p_storageId, p_size);
	}

	/**
	 * Create a block of memory in the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to allocate memory for.
	 * @return True if successful, false on failure (no space, element count exceeded or id used).
	 */
	public boolean superpeerStorageCreate(final DataStructure p_dataStructure) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			LOGGER.error("Invalid id 0x%X for data struct to allocate memory in superpeer storage",
					p_dataStructure.getID());
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return superpeerStorageCreate((int) p_dataStructure.getID(), p_dataStructure.sizeofObject());
	}

	/**
	 * Put data into an allocated block of memory in the superpeer storage.
	 *
	 * @param p_dataStructure Data structure to put with the storage id assigned.
	 * @return True if successful, false otherwise.
	 */
	public boolean superpeerStoragePut(final DataStructure p_dataStructure) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			LOGGER.error("Invalid id 0x%X for data struct to put data into superpeer storage", p_dataStructure.getID());
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return m_peer.superpeerStoragePut(p_dataStructure);
	}

	/**
	 * Get data from the superpeer storage.
	 *
	 * @param p_id Id of an allocated block to get the data from.
	 * @return Chunk with the data other null on error.
	 */
	public Chunk superpeerStorageGet(final int p_id) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return m_peer.superpeerStorageGet(p_id);
	}

	/**
	 * Get data from the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to read the data into.
	 * @return True on success, false on failure.
	 */
	public boolean superpeerStorageGet(final DataStructure p_dataStructure) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			LOGGER.error("Invalid id 0x%X for data struct to get data from superpeer storage", p_dataStructure.getID());
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return m_peer.superpeerStorageGet(p_dataStructure);
	}

	/**
	 * Remove an allocated block from the superpeer storage.
	 *
	 * @param p_id Storage id identifying the block to remove.
	 * @return True if successful, false otherwise.
	 */
	public boolean superpeerStorageRemove(final int p_id) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		m_peer.superpeerStorageRemove(p_id);
		return true;
	}

	/**
	 * Remove an allocated block from the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to remove.
	 * @return True if successful, false otherwise.
	 */
	public boolean superpeerStorageRemove(final DataStructure p_dataStructure) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			LOGGER.error("Invalid id 0x%X for data struct to remove data from superpeer storage",
					p_dataStructure.getID());
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		m_peer.superpeerStorageRemove((int) p_dataStructure.getID());
		return true;
	}

	/**
	 * Get the status of the superpeer storage.
	 *
	 * @return Status of the superpeer storage.
	 */
	public SuperpeerStorage.Status superpeerStorageGetStatus() {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed store data to a superpeer storage");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return m_peer.superpeerStorageGetStatus();
	}

	/**
	 * Replaces the backup peer for given range on responsible superpeer
	 */
	public void replaceBackupPeer(final long p_firstChunkIDOrRangeID, final short p_failedPeer,
			final short p_newPeer) {
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer is not allowed to change a peer's backup peers");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		m_peer.replaceBackupPeer(p_firstChunkIDOrRangeID, p_failedPeer, p_newPeer);
	}

	@Override
	public void eventTriggered(final AbstractEvent p_event) {
		if (p_event instanceof NodeFailureEvent) {
			NodeFailureEvent event = (NodeFailureEvent) p_event;

			if (event.getRole() == NodeRole.PEER) {
				m_chunkIDCacheTree.invalidatePeer(event.getNodeID());
			}
		} else if (p_event instanceof NameserviceCacheEntryUpdateEvent) {
			NameserviceCacheEntryUpdateEvent event = (NameserviceCacheEntryUpdateEvent) p_event;
			// update if available to avoid caching all entries
			if (m_applicationIDCache.contains(event.getId())) {
				m_applicationIDCache.put(event.getId(), event.getChunkID());
			}
		}
	}

	// --------------------------------------------------------------------------------

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_event = p_componentAccessor.getComponent(EventComponent.class);
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
	}

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		if (m_cachesEnabled) {
			m_chunkIDCacheTree = new CacheTree(m_maxCacheEntries, ORDER);

			// TODO: Check cache! If number of entries is smaller than number of entries in nameservice, bg won't
			// terminate.
			m_applicationIDCache = new Cache<>(m_nameserviceCacheEntries);
			// m_aidCache.enableTTL();
		}

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_superpeer = new OverlaySuperpeer(
					m_boot.getNodeID(),
					m_boot.getNodeIDBootstrap(),
					m_boot.getNumberOfAvailableSuperpeers(),
					m_pingInterval,
					m_maxBarriersPerSuperpeer,
					m_storageMaxNumEntries,
					(int) m_storageMaxSize.getBytes(),
					m_boot,
					m_network, m_event);
		} else {
			m_peer = new OverlayPeer(m_boot.getNodeID(), m_boot.getNodeIDBootstrap(),
					m_boot.getNumberOfAvailableSuperpeers(), m_boot,
					m_network, m_event);
			m_event.registerListener(this, NameserviceCacheEntryUpdateEvent.class);
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

	/**
	 * Clear the cache
	 */
	@SuppressWarnings("unused")
	private void clear() {
		m_chunkIDCacheTree = new CacheTree(m_maxCacheEntries, ORDER);
		m_applicationIDCache.clear();
	}
}
