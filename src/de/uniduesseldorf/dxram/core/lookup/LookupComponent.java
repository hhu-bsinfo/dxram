package de.uniduesseldorf.dxram.core.lookup;

import java.util.List;

import de.uniduesseldorf.dxram.core.backup.BackupRange;
import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;

public abstract class LookupComponent extends DXRAMComponent {
	
	public LookupComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Get the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @return the corresponding NodeID or null if node ID not available.
	 */
	public abstract Locations get(long p_chunkID);

	/**
	 * Returns all backup ranges (RangeID + backup peers) for given node
	 * @param p_nodeID
	 *            the NodeID
	 * @return all backup ranges for given node
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	public abstract BackupRange[] getAllBackupRanges(short p_nodeID);

	/**
	 * Returns all backup ranges (RangeID + backup peers) for given node
	 * @param p_owner
	 *            the NodeID of the recovered peer
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	public abstract void updateAllAfterRecovery(short p_owner);

	/**
	 * Store migration in meta-data management
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	public abstract void migrate(long p_chunkID, short p_nodeID);

	/**
	 * Store migration of ID range in meta-data management
	 * @param p_startCID
	 *            the first ID
	 * @param p_endCID
	 *            the last ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	public abstract void migrateRange(long p_startCID, long p_endCID, short p_nodeID);

	/**
	 * Store migration in meta-data management; Concerns not created chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	public abstract void migrateNotCreatedChunk(long p_chunkID, short p_nodeID);

	/**
	 * Store migration in meta-data management; Concerns own chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	public abstract void migrateOwnChunk(long p_chunkID, short p_nodeID);

	/**
	 * Initializes the given ID range in CIDTree
	 * @param p_firstChunkIDOrRangeID
	 *            the last ID
	 * @param p_locations
	 *            the NodeID of creator and backup nodes
	 * @throws LookupException
	 *             if the initialization could not be executed
	 */
	public abstract void initRange(long p_firstChunkIDOrRangeID, Locations p_locations);

	/**
	 * Remove the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @throws LookupException
	 *             if the NodeID could not be removed
	 */
	public abstract void remove(long p_chunkID);

	/**
	 * Remove the corresponding NodeIDs for the given IDs
	 * @param p_chunkIDs
	 *            the IDs
	 * @throws LookupException
	 *             if the NodeIDs could not be removed
	 */
	public abstract void remove(long[] p_chunkIDs);

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws LookupException
	 *             if the id could not be inserted
	 */
	public abstract void insertID(int p_id, long p_chunkID);

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @throws LookupException
	 *             if the id could not be found
	 * @return the ChunkID
	 */
	public abstract long getChunkID(int p_id);

	/**
	 * Return the number of identifier mappings
	 * @return the number of mappings
	 * @throws LookupException
	 *             if mapping count could not be gotten
	 */
	public abstract long getMappingCount();

	/**
	 * Invalidates the cache entry for given ChunkIDs
	 * @param p_chunkIDs
	 *            the IDs
	 */
	public abstract void invalidate(final long... p_chunkIDs);

	/**
	 * Invalidates the cache entry for given ChunkID range
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 */
	public abstract void invalidate(final long p_startCID, final long p_endCID);

	/**
	 * Returns if given node is still available as a peer
	 * @param p_creator
	 *            the creator's NodeID
	 * @return true if given node is available and a peer, false otherwise
	 */
	public abstract boolean creatorAvailable(final short p_creator);

	/**
	 * Verifies if there is another node in the superpeer overlay
	 * @return true if there is another node, false otherwise
	 */
	public abstract boolean isLastSuperpeer();

	/**
	 * Returns a list with all superpeers
	 * @return all superpeers
	 */
	public abstract List<Short> getSuperpeers();

	/**
	 * Checks if all superpeers are known
	 * @return true if all superpeers are known, false otherwise
	 */
	public abstract boolean overlayIsStable();
}
