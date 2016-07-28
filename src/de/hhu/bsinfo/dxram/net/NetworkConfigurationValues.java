
package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the network package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class NetworkConfigurationValues {
	/**
	 * Configuration values for the network component.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
	 */
	public static class Component {
		public static final Pair<String, Integer> THREAD_COUNT_MSG_HANDLER =
				new Pair<String, Integer>("ThreadCountDefaultMessageHandler", 1);
		public static final Pair<String, Integer> REQUEST_MAP_ENTRY_COUNT =
				new Pair<String, Integer>("RequestMapEntryCount", (int) Math.pow(2, 20));
		public static final Pair<String, Integer> INCOMING_BUFFER_SIZE =
				new Pair<String, Integer>("IncomingBufferSize", 1024 * 1024);
		public static final Pair<String, Integer> OUTGOING_BUFFER_SIZE =
				new Pair<String, Integer>("OutgoingBufferSize", 1024 * 1024);
		public static final Pair<String, Integer> NUMBER_OF_PENDING_BUFFERS_PER_CONNECTION =
				new Pair<String, Integer>("NumberOfPendingBuffersPerConnection", 100);
		public static final Pair<String, Integer> FLOW_CONTROL_WINDOW_SIZE =
				new Pair<String, Integer>("FlowControlWindowSize", 1024 * 1024);
		public static final Pair<String, Integer> REQUEST_TIMEOUT_MS =
				new Pair<String, Integer>("RequestTimeoutMs", 333);
	}
}
