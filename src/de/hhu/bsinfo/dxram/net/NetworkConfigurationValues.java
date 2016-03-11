package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.utils.Pair;

public class NetworkConfigurationValues {
	public static class Component {
		public static final Pair<String, Integer> THREAD_COUNT_MSG_HANDLER = new Pair<String, Integer>("ThreadCountMessageHandler", 4);
		public static final Pair<String, Integer> THREAD_COUNT_MSG_CREATOR = new Pair<String, Integer>("ThreadCountMessageCreator", 1);
		public static final Pair<String, Integer> MAX_OUTSTANDING_BYTES = new Pair<String, Integer>("MaxOutstandingBytes", 128 * 1024 * 1024);
		public static final Pair<String, Integer> NUMBER_OF_BUFFERS = new Pair<String, Integer>("NumberOfBuffers", 256);
		public static final Pair<String, Integer> REQUEST_TIMEOUT_MS = new Pair<String, Integer>("RequestTimeoutMs", 2000);
	}
}
