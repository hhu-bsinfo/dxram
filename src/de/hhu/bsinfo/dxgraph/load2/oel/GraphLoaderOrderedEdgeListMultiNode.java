
package de.hhu.bsinfo.dxgraph.load2.oel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.dxgraph.load2.RebaseVertexID;
import de.hhu.bsinfo.dxgraph.load2.oel.messages.GraphLoaderOrderedEdgeListMessages;
import de.hhu.bsinfo.dxgraph.load2.oel.messages.LoaderMasterBroadcastMessage;
import de.hhu.bsinfo.dxgraph.load2.oel.messages.LoaderMasterSyncGraphIndexMessage;
import de.hhu.bsinfo.dxgraph.load2.oel.messages.LoaderSlaveSignOnMessage;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.locks.SpinLock;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

// expecting every node to have a single .oel file
public class GraphLoaderOrderedEdgeListMultiNode extends GraphLoaderOrderedEdgeList implements MessageReceiver {

	private boolean m_masterLoader = false;
	private Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> m_localEdgeLists = null;
	private volatile GraphIndex m_graphIndex = new GraphIndex();

	// for master only
	private Lock m_signOnMutex = new SpinLock();

	// for slaves only
	private volatile short m_masterNodeID = NodeID.INVALID_ID;

	public GraphLoaderOrderedEdgeListMultiNode(final String p_path, final int p_numNodes, final int p_vertexBatchSize,
			final boolean p_masterLoader) {
		super(p_path, p_numNodes, p_vertexBatchSize);
		m_masterLoader = p_masterLoader;
	}

	@Override
	public boolean load(final String p_path, final int p_numNodes) {
		// network setup
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE,
				GraphLoaderOrderedEdgeListMessages.SUBTYPE_SLAVE_SIGN_ON, LoaderSlaveSignOnMessage.class);
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE,
				GraphLoaderOrderedEdgeListMessages.SUBTYPE_MASTER_SYNC_GRAPH_INDEX,
				LoaderMasterSyncGraphIndexMessage.class);
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE,
				GraphLoaderOrderedEdgeListMessages.SUB_TYPE_MASTER_BROADCAST, LoaderMasterBroadcastMessage.class);

		m_networkService.registerReceiver(LoaderSlaveSignOnMessage.class, this);
		m_networkService.registerReceiver(LoaderMasterSyncGraphIndexMessage.class, this);
		m_networkService.registerReceiver(LoaderMasterBroadcastMessage.class, this);

		m_localEdgeLists = setupEdgeLists(p_path);

		if (m_localEdgeLists.first().size() != 1) {
			m_loggerService.error(getClass(), "Having more than one edge list file (or none), expecting exactly one.");
			return false;
		}

		if (m_masterLoader) {
			// node count without master = slaves
			return master(m_localEdgeLists.first().get(0), p_numNodes - 1);
		} else {
			return slave(m_localEdgeLists.first().get(0));
		}
	}

	private boolean master(final OrderedEdgeList p_localEdgeLists, final int p_numSlaves) {
		m_loggerService.info(getClass(), "Running as master, waiting for " + p_numSlaves + " slaves to sign on...");

		// wait until all slaves signed on
		while (m_graphIndex.getIndex().size() < p_numSlaves) {
			// broadcast to all peers, which are potential slaves
			for (short peer : m_bootService.getAvailablePeerNodeIDs()) {
				LoaderMasterBroadcastMessage message = new LoaderMasterBroadcastMessage(peer);
				NetworkErrorCodes error = m_networkService.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(),
							"Sending broadcast message to peer " + peer + " failed: " + error);
				}
			}

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {}
		}

		m_loggerService.info(getClass(), "All slaves signed on, distributing graph index...");

		// add master as well to complete the index
		GraphIndex.Entry localEntry = new GraphIndex.Entry();
		localEntry.m_nodeIndex = p_localEdgeLists.getNodeIndex();
		localEntry.m_vertexCount = p_localEdgeLists.getTotalVertexCount();
		localEntry.m_nodeID = m_bootService.getNodeID();

		m_graphIndex.getIndex().add(localEntry);
		// ensure sorting for correct rebasing
		m_graphIndex.sort();

		// now sync the index to all slaves
		for (GraphIndex.Entry entry : m_graphIndex.getIndex()) {
			// don't sync to current node
			if (entry.m_nodeID != m_bootService.getNodeID()) {
				LoaderMasterSyncGraphIndexMessage message =
						new LoaderMasterSyncGraphIndexMessage(entry.m_nodeID, m_graphIndex);
				NetworkErrorCodes error = m_networkService.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(),
							"Sending sync graph index to " + entry.m_nodeID + " failed: " + error);
					return false;
				}
			}
		}

		m_loggerService.info(getClass(), "Starting local loading of graph...");

		return load(p_localEdgeLists, m_graphIndex);
	}

	private boolean slave(final OrderedEdgeList p_localEdgeLists) {
		m_loggerService.info(getClass(), "Running as slave, waiting for master broadcast...");

		// create index entry for sign on
		GraphIndex.Entry localEntry = new GraphIndex.Entry();
		localEntry.m_nodeIndex = p_localEdgeLists.getNodeIndex();
		localEntry.m_vertexCount = p_localEdgeLists.getTotalVertexCount();
		localEntry.m_nodeID = m_bootService.getNodeID();

		// invalidate for waiting later
		m_graphIndex = null;

		// wait for master's broadcast message
		while (m_masterNodeID == NodeID.INVALID_ID) {
			Thread.yield();
		}

		m_loggerService.info(getClass(), "Master broadcast received, signing on to " + m_masterNodeID + "...");

		// send sign on to master
		LoaderSlaveSignOnMessage message = new LoaderSlaveSignOnMessage(m_masterNodeID, localEntry);
		NetworkErrorCodes error = m_networkService.sendMessage(message);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_loggerService.error(getClass(), "Sending sign on to " + m_masterNodeID + " failed: " + error);
			return false;
		}

		m_loggerService.info(getClass(), "Waiting to receive graph index from master...");

		// wait to receive full graph index
		while (m_graphIndex == null) {
			Thread.yield();
		}

		m_loggerService.info(getClass(), "Sign on successful, got graph index, starting local load...");

		return load(p_localEdgeLists, m_graphIndex);
	}

	public static class GraphIndex implements Importable, Exportable, RebaseVertexID {
		private List<Entry> m_index = new ArrayList<Entry>();

		public GraphIndex() {

		}

		public void sort() {
			// sort list by node index to ensure correct rebasing for neighbors
			Collections.sort(m_index, new Comparator<Entry>() {
				@Override
				public int compare(Entry o1, Entry o2) {
					if (o1.m_nodeIndex < o2.m_nodeIndex) {
						return -1;
					} else if (o1.m_nodeIndex > o2.m_nodeIndex) {
						return 1;
					} else {
						return 0;
					}
				}
			});
		}

		public List<Entry> getIndex() {
			return m_index;
		}

		@Override
		public long rebase(long p_id) {
			// find section the vertex (of the neighbor) is in
			long globalVertexIDOffset = 0;
			for (Entry entry : m_index) {
				if (p_id >= globalVertexIDOffset && p_id <= globalVertexIDOffset + entry.m_vertexCount) {
					return ChunkID.getChunkID(entry.m_nodeID, p_id - globalVertexIDOffset);
				}

				globalVertexIDOffset += entry.m_vertexCount;
			}

			// out of range ID
			return ChunkID.INVALID_ID;
		}

		@Override
		public void rebase(long[] p_ids) {
			// utilize locality instead of calling function
			for (int i = 0; i < p_ids.length; i++) {
				// out of range ID, default assign if not found in loop
				long tmp = ChunkID.INVALID_ID;

				// find section the vertex (of the neighbor) is in
				long globalVertexIDOffset = 0;
				for (Entry entry : m_index) {
					if (p_ids[i] >= globalVertexIDOffset && p_ids[i] <= globalVertexIDOffset + entry.m_vertexCount) {
						tmp = ChunkID.getChunkID(entry.m_nodeID, p_ids[i] - globalVertexIDOffset);
						break;
					}

					globalVertexIDOffset += entry.m_vertexCount;
				}

				p_ids[i] = tmp;
			}
		}

		public static class Entry implements Importable, Exportable {
			public int m_nodeIndex;
			public long m_vertexCount;
			public short m_nodeID;

			public static int sizeofObjectStatic() {
				return Integer.BYTES + Long.BYTES + Short.BYTES;
			}

			@Override
			public int sizeofObject() {
				return sizeofObjectStatic();
			}

			@Override
			public boolean hasDynamicObjectSize() {
				return false;
			}

			@Override
			public int exportObject(Exporter p_exporter, int p_size) {
				p_exporter.writeInt(m_nodeIndex);
				p_exporter.writeLong(m_vertexCount);
				p_exporter.writeShort(m_nodeID);
				return sizeofObject();
			}

			@Override
			public int importObject(Importer p_importer, int p_size) {
				m_nodeIndex = p_importer.readInt();
				m_vertexCount = p_importer.readLong();
				m_nodeID = p_importer.readShort();
				return sizeofObject();
			}
		}

		@Override
		public int sizeofObject() {
			return Integer.BYTES + m_index.size() * Entry.sizeofObjectStatic();

		}

		@Override
		public boolean hasDynamicObjectSize() {
			return true;
		}

		@Override
		public int exportObject(Exporter p_exporter, int p_size) {
			p_exporter.writeInt(m_index.size());
			for (Entry entry : m_index) {
				p_exporter.exportObject(entry);
			}
			return sizeofObject();
		}

		@Override
		public int importObject(Importer p_importer, int p_size) {
			int count = p_importer.readInt();
			for (int i = 0; i < count; i++) {
				Entry entry = new Entry();
				p_importer.importObject(entry);
				m_index.add(entry);
			}
			return sizeofObject();
		}
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == GraphLoaderOrderedEdgeListMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case GraphLoaderOrderedEdgeListMessages.SUBTYPE_SLAVE_SIGN_ON:
						incomingSlaveSignOn((LoaderSlaveSignOnMessage) p_message);
						break;
					case GraphLoaderOrderedEdgeListMessages.SUBTYPE_MASTER_SYNC_GRAPH_INDEX:
						incomingMasterSyncGraphIndex((LoaderMasterSyncGraphIndexMessage) p_message);
						break;
					case GraphLoaderOrderedEdgeListMessages.SUB_TYPE_MASTER_BROADCAST:
						incomingMasterBroadcast((LoaderMasterBroadcastMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	/**
	 * Handles an incoming LoaderSlaveSignOn
	 * @param p_message
	 *            the incoming message
	 */
	private void incomingSlaveSignOn(final LoaderSlaveSignOnMessage p_message) {

		// make sure only one sign on at a time executes
		m_signOnMutex.lock();
		m_graphIndex.getIndex().add(p_message.getIndexEntry());
		m_signOnMutex.unlock();
	}

	/**
	 * Handles an incoming LoaderMasterSyncGraphIndex
	 * @param p_message
	 *            the incoming message
	 */
	private void incomingMasterSyncGraphIndex(final LoaderMasterSyncGraphIndexMessage p_message) {
		m_graphIndex = p_message.getIndex();
	}

	/**
	 * Handles an incoming LoaderMasterSyncGraphIndex
	 * @param p_message
	 *            the incoming message
	 */
	private void incomingMasterBroadcast(final LoaderMasterBroadcastMessage p_message) {
		m_masterNodeID = p_message.getSource();
	}
}
