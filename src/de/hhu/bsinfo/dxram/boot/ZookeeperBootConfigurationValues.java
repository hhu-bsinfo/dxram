package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.utils.Pair;

public class ZookeeperBootConfigurationValues {
	public static class Component {
		public static final Pair<String, String> PATH = new Pair<String, String>("Path", "/dxram");
		public static final Pair<String, String> CONNECTION_STRING = new Pair<String, String>("ConnectionString", "127.0.0.1:2181");
		public static final Pair<String, Integer> TIMEOUT = new Pair<String, Integer>("Timeout", 10000);
		public static final Pair<String, Integer> BITFIELD_SIZE = new Pair<String, Integer>("BitfieldSize", 256 * 1024);
	}
}