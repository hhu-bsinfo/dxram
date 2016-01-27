package de.hhu.bsinfo.dxram.event;

import de.hhu.bsinfo.utils.Pair;

public class EventConfigurationValues {
	public static class Component {
		public static final Pair<String, Boolean> USE_EXECUTOR = new Pair<String, Boolean>("UseExecutor", true);
		public static final Pair<String, Integer> THREAD_COUNT = new Pair<String, Integer>("ThreadCount", 1);
	}
}
