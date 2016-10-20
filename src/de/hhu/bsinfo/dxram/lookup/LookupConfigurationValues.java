
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the lookup service.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 15.03.16
 */
class LookupConfigurationValues {

	/**
	 * Backup component configuration attributes
	 */
	public static class Component {
		static final Pair<String, Boolean> CACHES_ENABLED = new Pair<>("CacheEnabled", true);
		static final Pair<String, Long> CACHE_ENTRIES = new Pair<>("CacheEntries", 1000L);
		static final Pair<String, Integer> NAMESERVICE_CACHE_ENTRIES =
				new Pair<>("NameserviceCacheEntries", 1000000);
		static final Pair<String, Long> CACHE_TTL = new Pair<>("CacheTTL", 1000L);
		static final Pair<String, Integer> PING_INTERVAL = new Pair<>("PingInterval", 1);
		static final Pair<String, Integer> MAX_BARRIERS_PER_SUPERPEER = new Pair<>("MaxBarriersPerSuperpeer", 1000);
		static final Pair<String, Integer> STORAGE_MAX_NUM_ENTRIES = new Pair<>("StorageMaxNumEntries", 1000);
		static final Pair<String, Integer> STORAGE_MAX_SIZE_BYTES = new Pair<>("StorageMaxSizeBytes", 32 * 1024 * 1024);
	}
}
