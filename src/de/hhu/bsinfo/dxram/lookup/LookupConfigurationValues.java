package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.utils.Pair;

public class LookupConfigurationValues {
	
	public static class Component {
		public static final Pair<String, Integer> PING_INTERVAL = new Pair<String, Integer>("PingInterval", 1);
		public static final Pair<String, Boolean> CACHES_ENABLED = new Pair<String, Boolean>("CacheEnabled", true);
		public static final Pair<String, Integer> CACHE_ENTRIES = new Pair<String, Integer>("CacheEntries", 1000);
		public static final Pair<String, Long> CACHE_TTL = new Pair<String, Long>("CacheTTL", 1000L);
		public static final Pair<String, Integer> NAMESERVICE_CACHE_ENTRIES = new Pair<String, Integer>("NameserviceCacheEntries", 1000000);
	}
	
	public static class Service {
		
	}
}
