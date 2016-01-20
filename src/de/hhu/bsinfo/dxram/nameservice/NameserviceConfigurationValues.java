package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.utils.Pair;

public class NameserviceConfigurationValues {
	public static class Component {
		public static final Pair<String, String> TYPE = new Pair<String, String>("Type", "NAME");
		public static final Pair<String, Integer> KEY_LENGTH = new Pair<String, Integer>("KeyLength", 32);
	}
}
