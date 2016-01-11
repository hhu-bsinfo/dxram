package de.uniduesseldorf.dxram.core.lookup;

import de.uniduesseldorf.utils.Pair;

public class LookupConfigurationValues {
	
	public static class Component {
		public static final Pair<String, Integer> SLEEP = new Pair<String, Integer>("Sleep", 1);
		public static final Pair<String, Integer> CACHE_ENTRIES = new Pair<String, Integer>("CacheEntries", 1000);
		public static final Pair<String, Long> CACHE_TTL = new Pair<String, Long>("CacheTTL", 1000L);
		public static final Pair<String, Integer> NAMESERVICE_CACHE_ENTRIES = new Pair<String, Integer>("NameserviceCacheEntries", 1000000);
	}
	
	public static class Service {
		
	}
}
