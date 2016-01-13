package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.utils.Pair;

public class NetworkConfigurationValues {
	public static class Component {
		public static final Pair<String, Integer> MSG_BUFFER_SIZE = new Pair<String, Integer>("MessageBufferSize", 32 * 1024);
		public static final Pair<String, Integer> THREAD_COUNT_MSG_HANDLER = new Pair<String, Integer>("ThreadCountMessageHandler", 10);
		public static final Pair<String, Integer> THREAD_COUNT_TASK_HANDLER = new Pair<String, Integer>("ThreadCountTaskHandler", 10);
		public static final Pair<String, Boolean> STATISTICS_THROUGHPUT = new Pair<String, Boolean>("StatisticsThroughput", true);
		public static final Pair<String, Boolean> STATISTICS_REQUESTS = new Pair<String, Boolean>("StatisticsRequests", true);
	}
}
