
package de.uniduesseldorf.dxram.core.lookup;

import java.util.List;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkHandler.BackupRange;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Location;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.Cache;
import de.uniduesseldorf.dxram.utils.Cache.EvictionPolicy;
import de.uniduesseldorf.dxram.utils.Cache.POLICY;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Access meta data through a cache
 * @author Florian Klein
 *         09.03.2012
 */
public final class CachedLookup implements LookupInterface {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(CachedLookup.class);

	// Attributes
	private LookupInterface m_lookup;
	private Cache<Long, Short> m_chunkIDCache;
	private Cache<Long, Long> m_applicationIDCache;

	// Constructors
	/**
	 * Creates an instance of CachedLookup
	 */
	public CachedLookup() {
		this(new LookupHandler(), new Cache<Long, Short>(POLICY.LRU));
	}

	/**
	 * Creates an instance of CachedLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 */
	public CachedLookup(final LookupInterface p_lookup) {
		this(p_lookup, new Cache<Long, Short>());
	}

	/**
	 * Creates an instance of CachedLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 * @param p_maxSize
	 *            the maximum of cached elements
	 */
	public CachedLookup(final LookupInterface p_lookup, final int p_maxSize) {
		this(p_lookup, new Cache<Long, Short>(p_maxSize));
	}

	/**
	 * Creates an instance of CachedLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 * @param p_policy
	 *            the eviction policy
	 */
	public CachedLookup(final LookupInterface p_lookup, final EvictionPolicy<Long, Short> p_policy) {
		this(p_lookup, new Cache<Long, Short>(p_policy));
	}

	/**
	 * Creates an instance of CachedLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 * @param p_maxSize
	 *            the maximum of cached elements
	 * @param p_policy
	 *            the eviction policy
	 */
	public CachedLookup(final LookupInterface p_lookup, final int p_maxSize, final EvictionPolicy<Long, Short> p_policy) {
		this(p_lookup, new Cache<Long, Short>(p_maxSize, p_policy));
	}

	/**
	 * Creates an instance of CachedLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 * @param p_cache
	 *            the cache
	 */
	public CachedLookup(final LookupInterface p_lookup, final Cache<Long, Short> p_cache) {
		Contract.checkNotNull(p_lookup, "no lookup service given");
		Contract.checkNotNull(p_cache, "no cache given");

		m_lookup = p_lookup;
		m_chunkIDCache = p_cache;
		m_applicationIDCache = new Cache<Long, Long>(10000);
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		m_lookup.initialize();
		if (!NodeID.getRole().equals(Role.SUPERPEER)) {
			m_chunkIDCache.enableTTL();
			m_applicationIDCache.enableTTL();
		} else {
			m_chunkIDCache = null;
			m_applicationIDCache = null;
		}
	}

	@Override
	public void close() {
		m_lookup.close();
	}

	@Override
	public Location get(final long p_chunkID) throws LookupException {
		return get(p_chunkID, false);
	}

	@Override
	public BackupRange[] getAllBackupRanges(final short p_nodeID) throws LookupException {
		return m_lookup.getAllBackupRanges(p_nodeID);
	}

	/**
	 * Get the corresponding primary peer and backup peers for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @param p_force
	 *            writes a new entry if no entry exists
	 * @return the corresponding primary peer and backup peers
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	public Location get(final long p_chunkID, final boolean p_force) throws LookupException {
		Short primaryPeer = -1;
		Location ret;

		ChunkID.check(p_chunkID);

		primaryPeer = m_chunkIDCache.get(p_chunkID);
		if (p_force || -1 == primaryPeer) {
			LOGGER.trace("value not cached: " + p_chunkID);

			ret = m_lookup.get(p_chunkID);

			m_chunkIDCache.put(p_chunkID, ret.getPrimaryPeer());
		} else {
			ret = new Location(primaryPeer, new long[] {-1, -1});
		}

		return ret;
	}

	@Override
	public void updateAllAfterRecovery(final short p_owner) throws LookupException {
		m_lookup.updateAllAfterRecovery(p_owner);
	}

	@Override
	public void migrate(final long p_chunkID, final short p_nodeID) throws LookupException {
		ChunkID.check(p_chunkID);

		invalidate(p_chunkID);

		m_lookup.migrate(p_chunkID, p_nodeID);
	}

	@Override
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) throws LookupException {
		ChunkID.check(p_startCID);
		ChunkID.check(p_endCID);

		invalidate(p_startCID, p_endCID);

		m_lookup.migrateRange(p_startCID, p_endCID, p_nodeID);
	}

	@Override
	public void migrateNotCreatedChunk(final long p_chunkID, final short p_nodeID) throws LookupException {
		ChunkID.check(p_chunkID);

		invalidate(p_chunkID);

		m_lookup.migrateNotCreatedChunk(p_chunkID, p_nodeID);
	}

	@Override
	public void migrateOwnChunk(final long p_chunkID, final short p_nodeID) throws LookupException {
		ChunkID.check(p_chunkID);

		invalidate(p_chunkID);

		m_lookup.migrateOwnChunk(p_chunkID, p_nodeID);
	}

	@Override
	public void initRange(final long p_firstChunkID, final Locations p_locations) throws LookupException {
		ChunkID.check(p_firstChunkID);

		m_lookup.initRange(p_firstChunkID, p_locations);
	}

	@Override
	public void insertID(final int p_id, final long p_chunkID) throws LookupException {
		m_lookup.insertID(p_id, p_chunkID);
	}

	@Override
	public long getChunkID(final int p_id) throws LookupException {
		long ret;
		Long chunkID;

		chunkID = m_applicationIDCache.get((long) p_id);
		if (null == chunkID) {
			LOGGER.trace("value not cached: " + p_id);

			ret = m_lookup.getChunkID(p_id);

			m_applicationIDCache.put((long) p_id, ret);
		} else {
			ret = chunkID.longValue();
		}
		return ret;
	}

	@Override
	public long getMappingCount() throws LookupException {
		return m_lookup.getMappingCount();
	}

	@Override
	public void remove(final long p_chunkID) throws LookupException {
		ChunkID.check(p_chunkID);

		invalidate(p_chunkID);

		m_lookup.remove(p_chunkID);
	}

	@Override
	public void remove(final long[] p_chunkIDs) throws LookupException {
		ChunkID.check(p_chunkIDs);

		invalidate(p_chunkIDs);

		m_lookup.remove(p_chunkIDs);
	}

	@Override
	public void invalidate(final long... p_chunkIDs) {
		for (long chunkID : p_chunkIDs) {
			m_chunkIDCache.remove(chunkID);
		}
	}

	@Override
	public void invalidate(final long p_startCID, final long p_endCID) {
		long iter = p_startCID;
		while (iter <= p_endCID) {
			invalidate(iter++);
		}
	}

	@Override
	public boolean creatorAvailable(final short p_creator) {
		return m_lookup.creatorAvailable(p_creator);
	}

	@Override
	public boolean isLastSuperpeer() {
		return m_lookup.isLastSuperpeer();
	}

	@Override
	public List<Short> getSuperpeers() {
		return m_lookup.getSuperpeers();
	}

	@Override
	public boolean overlayIsStable() {
		return m_lookup.overlayIsStable();
	}

	/**
	 * Clear the cache
	 */
	public void clear() {
		m_chunkIDCache.clear();
	}

}
