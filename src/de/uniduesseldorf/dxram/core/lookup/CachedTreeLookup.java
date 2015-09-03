
package de.uniduesseldorf.dxram.core.lookup;

import java.util.List;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.storage.CacheTree;
import de.uniduesseldorf.dxram.utils.Cache;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Access meta data through a cache
 * @author Florian Klein
 *         09.03.2012
 */
public final class CachedTreeLookup implements LookupInterface {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(CachedTreeLookup.class);

	private static final short ORDER = 10;
	private static final int NS_CACHE_SIZE = Core.getConfiguration().getIntValue(
			ConfigurationConstants.LOOKUP_NS_CACHE_SIZE);

	// Attributes
	private LookupInterface m_lookup;

	private CacheTree m_cidCacheTree;
	private Cache<Integer, Long> m_aidCache;

	// Constructors
	/**
	 * Creates an instance of CachedTreeLookup
	 */
	public CachedTreeLookup() {
		this(new LookupHandler());
	}

	/**
	 * Creates an instance of CachedTreeLookup
	 * @param p_lookup
	 *            the underlying LookupInterface
	 */
	public CachedTreeLookup(final LookupInterface p_lookup) {
		Contract.checkNotNull(p_lookup, "no lookup service given");

		m_lookup = p_lookup;

		m_cidCacheTree = null;
		m_aidCache = null;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		m_lookup.initialize();

		if (!NodeID.getRole().equals(Role.SUPERPEER)) {
			m_cidCacheTree = new CacheTree(ORDER);
			m_aidCache = new Cache<Integer, Long>(NS_CACHE_SIZE);
			// m_aidCache.enableTTL();
		}
	}

	@Override
	public void initChunkHandler() throws DXRAMException {
		m_lookup.initChunkHandler();
	}

	@Override
	public void close() {
		m_lookup.close();

		if (m_cidCacheTree != null) {
			m_cidCacheTree.close();
			m_cidCacheTree = null;
		}
		if (m_aidCache != null) {
			m_aidCache.clear();
			m_aidCache = null;
		}
	}


	
	@Override
	public Locations get(final long p_chunkID) throws LookupException {
		Locations ret;
		short nodeID;

		ret = m_cidCacheTree.getMetadata(p_chunkID);
		if (ret == null) {
			ret = m_lookup.get(p_chunkID);
			if (ret != null) {
				nodeID = ret.getPrimaryPeer();

				m_cidCacheTree.cacheRange(((long) nodeID << 48) + ret.getRange()[0],
						((long) nodeID << 48) + ret.getRange()[1], nodeID);
			}
		}
		return ret;
	}

	@Override
	public void migrate(final long p_chunkID, final short p_nodeID) throws LookupException {
		ChunkID.check(p_chunkID);

		invalidate(p_chunkID);

		m_lookup.migrate(p_chunkID, p_nodeID);
	}

	@Override
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID)
			throws LookupException {
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
		m_aidCache.put(p_id, p_chunkID);
		m_lookup.insertID(p_id, p_chunkID);
	}

	@Override
	public long getChunkID(final int p_id) throws LookupException {
		long ret;
		Long chunkID;

		chunkID = m_aidCache.get(p_id);
		if (null == chunkID) {
			LOGGER.trace("value not cached: " + p_id);

			ret = m_lookup.getChunkID(p_id);

			m_aidCache.put(p_id, ret);
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
	public void invalidate(final long... p_chunkIDs) {
		for (long chunkID : p_chunkIDs) {
			m_cidCacheTree.invalidateChunkID(chunkID);
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
		m_cidCacheTree = new CacheTree(ORDER);
		m_aidCache.clear();
	}

}
