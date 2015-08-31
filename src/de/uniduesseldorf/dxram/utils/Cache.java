
package de.uniduesseldorf.dxram.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;

/**
 * Implements a Cache with an optional eviction policy and an optional timeout
 * @author Florian Klein
 *         09.03.2012
 * @param <KeyType>
 *            Type of the key
 * @param <ValueType>
 *            Type of the value
 */
public class Cache<KeyType, ValueType> {

	// Attributes
	private Map<KeyType, CacheEntry<KeyType, ValueType>> m_map;
	private final int m_maxSize;
	private EvictionPolicy<KeyType, ValueType> m_policy;
	private TTLHandler m_ttlHandler;

	private ReadWriteLock m_lock;

	// Constructors
	/**
	 * Creates an instance of Cache
	 */
	public Cache() {
		this(Integer.MAX_VALUE, new LRUPolicy<KeyType, ValueType>());
	}

	/**
	 * Creates an instance of Cache
	 * @param p_policy
	 *            the eviction policy
	 */
	public Cache(final EvictionPolicy<KeyType, ValueType> p_policy) {
		this(Integer.MAX_VALUE, p_policy);
	}

	/**
	 * Creates an instance of Cache
	 * @param p_maxSize
	 *            the maximum of cached elements
	 */
	public Cache(final int p_maxSize) {
		this(p_maxSize, new LRUPolicy<KeyType, ValueType>());
	}

	/**
	 * Creates an instance of Cache
	 * @param p_policyEnum
	 *            the POLICY
	 */
	public Cache(final POLICY p_policyEnum) {
		final int maxSize = Core.getConfiguration().getIntValue(ConfigurationConstants.LOOKUP_CACHE_ENTRIES);
		EvictionPolicy<KeyType, ValueType> policy = null;

		Contract.check(maxSize > 0, "max size must be greater or equal 1");
		Contract.checkNotNull(p_policyEnum, "policy unkown");

		switch (p_policyEnum) {
		case DUMMY:
			policy = new DummyPolicy<KeyType, ValueType>();
			break;
		case LRU:
			policy = new LRUPolicy<KeyType, ValueType>();
			break;
		default:
			break;
		}

		m_map = new HashMap<KeyType, CacheEntry<KeyType, ValueType>>();
		m_maxSize = maxSize;
		m_policy = policy;
		m_ttlHandler = null;

		m_lock = new ReentrantReadWriteLock();
	}

	/**
	 * Creates an instance of Cache
	 * @param p_maxSize
	 *            the maximum of cached elements
	 * @param p_policy
	 *            the eviction policy
	 */
	public Cache(final int p_maxSize, final EvictionPolicy<KeyType, ValueType> p_policy) {
		Contract.check(p_maxSize > 0, "max size must be greater or equal 1");
		Contract.checkNotNull(p_policy, "no policy given");

		m_map = new HashMap<KeyType, CacheEntry<KeyType, ValueType>>();
		m_maxSize = p_maxSize;
		m_policy = p_policy;
		m_ttlHandler = null;

		m_lock = new ReentrantReadWriteLock(false);
	}

	// Methods
	/**
	 * Creates a new cache entry or updates an existing one
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public final void put(final KeyType p_key, final ValueType p_value) {
		CacheEntry<KeyType, ValueType> entry;

		Contract.checkNotNull(p_key, "no key given");

		m_lock.writeLock().lock();

		if (m_map.containsKey(p_key)) {
			entry = m_map.get(p_key);
			entry.m_value = p_value;
		} else {
			if (m_map.size() >= m_maxSize) {
				m_map.remove(m_policy.evict(m_map.values()));
			}

			entry = new CacheEntry<KeyType, ValueType>(p_key, p_value);
			m_policy.newEntry(entry);

			m_map.put(p_key, entry);
		}

		accessEntry(entry);

		m_lock.writeLock().unlock();
	}

	/**
	 * Gets the value of a cache entry for the given key
	 * @param p_key
	 *            the key
	 * @return the value of the cache entry or null if no entry exists
	 */
	public final ValueType get(final KeyType p_key) {
		ValueType ret = null;
		CacheEntry<KeyType, ValueType> entry;

		Contract.checkNotNull(p_key, "no key given");

		m_lock.readLock().lock();

		entry = m_map.get(p_key);
		if (entry != null) {
			accessEntry(entry);

			ret = entry.getValue();
		}

		m_lock.readLock().unlock();

		return ret;
	}

	/**
	 * Removes the cache entry for the given key
	 * @param p_key
	 *            the key
	 */
	public final void remove(final KeyType p_key) {
		Contract.checkNotNull(p_key, "no key given");

		m_lock.writeLock().lock();

		m_map.remove(p_key);

		m_lock.writeLock().unlock();
	}

	/**
	 * Checks if a cache entry exists for the given key
	 * @param p_key
	 *            the key
	 * @return true if a cahce entry exists, false otherwise
	 */
	public final boolean contains(final KeyType p_key) {
		boolean ret;

		Contract.checkNotNull(p_key, "no key given");

		m_lock.readLock().lock();

		ret = m_map.containsKey(p_key);

		m_lock.readLock().unlock();

		return ret;
	}

	/**
	 * Removes all entries from the cache
	 */
	public final void clear() {
		m_lock.writeLock().lock();

		m_map.clear();

		m_lock.writeLock().unlock();
	}

	/**
	 * Enables TTL for cache entries
	 */
	public final synchronized void enableTTL() {
		Thread t;
		long ttl;

		ttl = Math.max(Core.getConfiguration().getLongValue(ConfigurationConstants.LOOKUP_CACHE_TTL), 1000);

		if (m_ttlHandler != null && m_ttlHandler.isRunning()) {
			m_ttlHandler.stop();
		}

		m_ttlHandler = new TTLHandler(ttl);

		t = new Thread(m_ttlHandler);
		t.setName(TTLHandler.class.getSimpleName() + " for " + Cache.class.getSimpleName());
		t.setDaemon(true);
		t.start();
	}

	/**
	 * Disables TTL for cache entries
	 */
	public final synchronized void disableTTL() {
		if (m_ttlHandler != null && m_ttlHandler.isRunning()) {
			m_ttlHandler.stop();
		}
	}

	/**
	 * Access an cache entry
	 * @param p_entry
	 *            the cache entry
	 */
	private void accessEntry(final CacheEntry<KeyType, ValueType> p_entry) {
		p_entry.access();

		m_policy.accessEntry(p_entry);
	}

	// Classes
	/**
	 * Values indicate which cache policy should be used
	 * @author Kevin Beineke
	 *         08.09.2013
	 */
	public enum POLICY {
		DUMMY, LRU
	}

	/**
	 * Represents an cache entry
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static final class CacheEntry<KeyType, ValueType> {

		// Attribute
		private KeyType m_key;
		private ValueType m_value;
		private long m_created;
		private long m_lastAccess;
		private int m_accesses;
		private long m_flags;

		// Constructors
		/**
		 * Creates an instance of CacheEntry
		 * @param p_key
		 *            the key
		 * @param p_value
		 *            the value
		 */
		private CacheEntry(final KeyType p_key, final ValueType p_value) {
			m_key = p_key;
			m_value = p_value;
			m_created = System.currentTimeMillis();
			m_lastAccess = 0;
			m_accesses = 0;
			m_flags = 0;
		}

		// Getters
		/**
		 * Gets the key
		 * @return the key
		 */
		public KeyType getKey() {
			return m_key;
		}

		/**
		 * Gets the value
		 * @return the value
		 */
		public ValueType getValue() {
			return m_value;
		}

		/**
		 * Gets the creation time
		 * @return the creation time
		 */
		public long getCreated() {
			return m_created;
		}

		/**
		 * Gets the last access time
		 * @return the last access time
		 */
		public long getLastAccess() {
			return m_lastAccess;
		}

		/**
		 * Gets the access count
		 * @return the access count
		 */
		public int getAccesses() {
			return m_accesses;
		}

		/**
		 * Gets the flags
		 * @return the flags
		 */
		public long getFlags() {
			return m_flags;
		}

		// Setters
		/**
		 * Sets the flags
		 * @param p_flags
		 *            the flags
		 */
		public void setFlags(final long p_flags) {
			m_flags = p_flags;
		}

		// Methods
		/**
		 * Accesses the entry
		 */
		private void access() {
			m_lastAccess = System.currentTimeMillis();
			m_accesses++;
		}

		/**
		 * Get the String representation
		 * @return the String representation
		 */
		@Override
		public String toString() {
			return "[" + m_key + "] - " + m_value;
		}

	}

	/**
	 * Manages the entry timouts
	 * @author Florian Klein
	 *         09.03.2012
	 */
	private class TTLHandler implements Runnable {

		// Constants
		private static final long SLEEP_TIME = 1000;

		// Attributes
		private long m_ttl;

		private boolean m_running;

		// Constructors
		/**
		 * Creates an instance of TTLHandler
		 * @param p_ttl
		 *            the TTL value
		 */
		public TTLHandler(final long p_ttl) {
			m_ttl = p_ttl;

			m_running = false;
		}

		// Getters
		/**
		 * Checks if the TTLHandler is running
		 * @return true if the TTLHandler is running, false otherwise
		 */
		public final boolean isRunning() {
			return m_running;
		}

		// Methods
		/**
		 * When an object implementing interface <code>Runnable</code> is used
		 * to create a thread, starting the thread causes the object's <code>run</code> method to be called in that
		 * separately executing
		 * thread.
		 * <p>
		 * The general contract of the method <code>run</code> is that it may take any action whatsoever.
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			Iterator<CacheEntry<KeyType, ValueType>> iter;
			long time;

			m_running = true;
			while (m_running) {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (final InterruptedException e) {}

				if (m_running) {
					time = System.currentTimeMillis();

					m_lock.writeLock().lock();

					iter = m_map.values().iterator();
					while (iter.hasNext()) {
						if (time - iter.next().m_lastAccess > m_ttl) {
							iter.remove();
						}
					}

					m_lock.writeLock().unlock();
				}
			}
		}

		/**
		 * Stops the TTLHandler
		 */
		public void stop() {
			m_running = false;
		}

	}

	/**
	 * Methods for an ecivtion policy
	 * @author Florian Klein
	 *         09.03.2012
	 * @param <KeyType>
	 *            Type of the key
	 * @param <ValueType>
	 *            Type of the value
	 */
	public interface EvictionPolicy<KeyType, ValueType> {

		// Methods
		/**
		 * Defines the key of the cache entry which should be removed
		 * @param p_entries
		 *            the current cache entries
		 * @return return the key to remove
		 */
		KeyType evict(Collection<CacheEntry<KeyType, ValueType>> p_entries);

		/**
		 * A new cache entry was created
		 * @param p_entry
		 *            the created cache entry
		 */
		void newEntry(CacheEntry<KeyType, ValueType> p_entry);

		/**
		 * A cache entry was accessed
		 * @param p_entry
		 *            the accessed cache entry
		 */
		void accessEntry(CacheEntry<KeyType, ValueType> p_entry);

		/**
		 * A cache entry was removed
		 * @param p_entry
		 *            the removed cache entry
		 * @param p_entries
		 *            the current cache entries
		 */
		void removeEntry(CacheEntry<KeyType, ValueType> p_entry, Collection<CacheEntry<KeyType, ValueType>> p_entries);

	}

	/**
	 * Eviction policy, which removes always the first entry
	 * @author Florian Klein
	 *         09.03.2012
	 * @param <KeyType>
	 *            Type of the key
	 * @param <ValueType>
	 *            Type of the value
	 */
	private static class DummyPolicy<KeyType, ValueType> implements EvictionPolicy<KeyType, ValueType> {

		// Constrcutors
		/**
		 * Creates an instance of DummyPolicy
		 */
		public DummyPolicy() {}

		// Methods
		/**
		 * Defines the key of the cache entry which should be removed
		 * @param p_entries
		 *            the current cache entries
		 * @return return the key to remove
		 */
		@Override
		public KeyType evict(final Collection<CacheEntry<KeyType, ValueType>> p_entries) {
			return p_entries.iterator().next().getKey();
		}

		/**
		 * A new cache entry was created
		 * @param p_entry
		 *            the created cache entry
		 */
		@Override
		public void newEntry(final CacheEntry<KeyType, ValueType> p_entry) {}

		/**
		 * A cache entry was accessed
		 * @param p_entry
		 *            the accessed cache entry
		 */
		@Override
		public void accessEntry(final CacheEntry<KeyType, ValueType> p_entry) {}

		/**
		 * A cache entry was removed
		 * @param p_entry
		 *            the removed cache entry
		 * @param p_entries
		 *            the current cache entries
		 */
		@Override
		public void removeEntry(final CacheEntry<KeyType, ValueType> p_entry,
				final Collection<CacheEntry<KeyType, ValueType>> p_entries) {}

	}

	/**
	 * Eviction policy, which removes always the least recently used entry
	 * @author Kevin Beineke
	 *         08.09.2013
	 * @param <KeyType>
	 *            Type of the key
	 * @param <ValueType>
	 *            Type of the value
	 */
	private static class LRUPolicy<KeyType, ValueType> implements EvictionPolicy<KeyType, ValueType> {

		// Constrcutors
		/**
		 * Creates an instance of LRUPolicy
		 */
		public LRUPolicy() {}

		// Methods
		/**
		 * Defines the key of the cache entry which should be removed
		 * @param p_entries
		 *            the current cache entries
		 * @return return the key to remove
		 */
		@Override
		public KeyType evict(final Collection<CacheEntry<KeyType, ValueType>> p_entries) {
			Iterator<CacheEntry<KeyType, ValueType>> iter;
			CacheEntry<KeyType, ValueType> currentEntry;
			CacheEntry<KeyType, ValueType> leastRecentlyUsedEntry = null;
			long lastAccess = System.currentTimeMillis();

			iter = p_entries.iterator();
			while (iter.hasNext()) {
				currentEntry = iter.next();
				if (lastAccess > currentEntry.m_lastAccess) {
					leastRecentlyUsedEntry = currentEntry;
					lastAccess = currentEntry.m_lastAccess;
				}
			}
			return leastRecentlyUsedEntry.getKey();
		}

		/**
		 * A new cache entry was created
		 * @param p_entry
		 *            the created cache entry
		 */
		@Override
		public void newEntry(final CacheEntry<KeyType, ValueType> p_entry) {}

		/**
		 * A cache entry was accessed
		 * @param p_entry
		 *            the accessed cache entry
		 */
		@Override
		public void accessEntry(final CacheEntry<KeyType, ValueType> p_entry) {}

		/**
		 * A cache entry was removed
		 * @param p_entry
		 *            the removed cache entry
		 * @param p_entries
		 *            the current cache entries
		 */
		@Override
		public void removeEntry(final CacheEntry<KeyType, ValueType> p_entry,
				final Collection<CacheEntry<KeyType, ValueType>> p_entries) {}

	}

}
