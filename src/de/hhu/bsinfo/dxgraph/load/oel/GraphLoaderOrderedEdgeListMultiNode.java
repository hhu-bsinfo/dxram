package de.hhu.bsinfo.dxgraph.load.oel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.oel.messages.GraphLoaderOrderedEdgeListMessages;
import de.hhu.bsinfo.dxgraph.load.oel.messages.LoaderMasterBroadcastMessage;
import de.hhu.bsinfo.dxgraph.load.oel.messages.LoaderMasterSyncGraphIndexMessage;
import de.hhu.bsinfo.dxgraph.load.oel.messages.LoaderSlaveSignOnMessage;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;
import de.hhu.bsinfo.utils.locks.SpinLock;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;


// expecting every node to have a single .oel file
public class GraphLoaderOrderedEdgeListMultiNode extends GraphLoaderOrderedEdgeList implements MessageReceiver {

	private boolean m_masterLoader = false;
	private List<OrderedEdgeList> m_localEdgeLists = null;
	private volatile GraphIndex m_graphIndex = new GraphIndex();
	
	// for master only
	private Lock m_signOnMutex = new SpinLock();
	
	// for slaves only
	private volatile short m_masterNodeID = NodeID.INVALID_ID;
	
	public GraphLoaderOrderedEdgeListMultiNode(final String p_path, final int p_numNodes, final int p_vertexBatchSize, final boolean p_masterLoader)
	{
		super(p_path, p_numNodes, p_vertexBatchSize);
		m_masterLoader = p_masterLoader;
	}
	
	@Override
	public boolean load(final String p_path, final int p_numNodes) 
	{		
		// network setup
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUBTYPE_SLAVE_SIGN_ON, LoaderSlaveSignOnMessage.class);
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUBTYPE_MASTER_SYNC_GRAPH_INDEX, LoaderMasterSyncGraphIndexMessage.class);
		m_networkService.registerMessageType(GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUB_TYPE_MASTER_BROADCAST, LoaderMasterBroadcastMessage.class);
		
		m_networkService.registerReceiver(LoaderSlaveSignOnMessage.class, this);
		m_networkService.registerReceiver(LoaderMasterSyncGraphIndexMessage.class, this);
		m_networkService.registerReceiver(LoaderMasterBroadcastMessage.class, this);
		
		m_localEdgeLists = setupEdgeLists(p_path);
		
		if (m_localEdgeLists.size() != 1) {
			m_loggerService.error(getClass(), "Having more than one edge list file (or none), expecting exactly one.");
			return false;
		}
		
		if (m_masterLoader) {
			// node count without master = slaves
			return master(m_localEdgeLists.get(0), p_numNodes - 1); 
		} else {
			return slave(m_localEdgeLists.get(0));
		}
	}
	
	private boolean master(final OrderedEdgeList p_localEdgeLists, final int p_numSlaves)
	{
		m_loggerService.info(getClass(), "Running as master, waiting for " + p_numSlaves + " slaves to sign on...");
		
		// wait until all slaves signed on
		while (m_graphIndex.getIndex().size() < p_numSlaves)
		{
			// broadcast to all peers, which are potential slaves
			for (short peer : m_bootService.getAvailablePeerNodeIDs())
			{
				LoaderMasterBroadcastMessage message = new LoaderMasterBroadcastMessage(peer);
				NetworkErrorCodes error = m_networkService.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(), "Sending broadcast message to peer " + peer + " failed: " + error);
				} 
			}

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
		}
		
		m_loggerService.info(getClass(), "All slaves signed on, distributing graph index...");
		
		// add master as well to complete the index
		GraphIndex.Entry localEntry = new GraphIndex.Entry();
		localEntry.m_nodeIndex = p_localEdgeLists.getNodeIndex();
		localEntry.m_vertexCount = p_localEdgeLists.getTotalVertexCount();
		localEntry.m_nodeID = m_bootService.getNodeID();
		
		m_graphIndex.getIndex().add(localEntry);
		
		// now sync the index to all slaves
		for (GraphIndex.Entry entry : m_graphIndex.getIndex()) {
			// don't sync to current node
			if (entry.m_nodeID != m_bootService.getNodeID()) {
				LoaderMasterSyncGraphIndexMessage message = new LoaderMasterSyncGraphIndexMessage(entry.m_nodeID, m_graphIndex);
				NetworkErrorCodes error = m_networkService.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(), "Sending sync graph index to " + entry.m_nodeID + " failed: " + error);
					return false;
				} 
			}
		}
		
		m_loggerService.info(getClass(), "Starting local loading of graph...");
		
		return load(p_localEdgeLists, 0);
	}
	
	private boolean slave(final OrderedEdgeList p_localEdgeLists)
	{
		m_loggerService.info(getClass(), "Running as slave, waiting for master broadcast...");
		
		// create index entry for sign on
		GraphIndex.Entry localEntry = new GraphIndex.Entry();
		localEntry.m_nodeIndex = p_localEdgeLists.getNodeIndex();
		localEntry.m_vertexCount = p_localEdgeLists.getTotalVertexCount();
		localEntry.m_nodeID = m_bootService.getNodeID();
		
		// invalidate for waiting later
		m_graphIndex = null;
		
		// wait for master's broadcast message
		while (m_masterNodeID == NodeID.INVALID_ID) 
		{
			Thread.yield();
		}
		
		m_loggerService.info(getClass(), "Master broadcast received, signing on to " + m_masterNodeID + "...");
		
		// send sign on to master
		LoaderSlaveSignOnMessage message = new LoaderSlaveSignOnMessage(m_masterNodeID, localEntry);
		NetworkErrorCodes error = m_networkService.sendMessage(message);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_loggerService.error(getClass(), "Sending sign on to " + m_masterNodeID+ " failed: " + error);
			return false;
		} 
		
		m_loggerService.info(getClass(), "Waiting to receive graph index from master...");
		
		// wait to receive full graph index
		while (m_graphIndex == null)
		{
			Thread.yield();
		}
		
		m_loggerService.info(getClass(), "Sign on successful, got graph index, starting local load...");
		
		return load(p_localEdgeLists, 0);
	}
	
	
	protected boolean loadWithGraphIndex(final OrderedEdgeList p_orderedEdgeList)
	{
		// loading our own local data
		// re-basing neighbour entries using the graph index
		
		Vertex[] vertexBuffer = new Vertex[m_vertexBatchSize];
		int readCount = 0;
		boolean loop = true;
		while (loop)
		{
			while (readCount < vertexBuffer.length)
			{
				Vertex vertex = p_orderedEdgeList.readVertex();
				if (vertex == null) {
					break;
				}
				
				// re-base neighbours
				List<Long> neighbours = vertex.getNeighbours();
				for (int i = 0; i < neighbours.size(); i++) {
					neighbours.set(i, m_graphIndex.rebaseNeighborLocalID(neighbours.get(i), p_orderedEdgeList.getNodeIndex()));
				}
			
				vertexBuffer[readCount] = vertex;
				readCount++;
			}
			
			// create an array which is filled without null padding at the end
			// if necessary 
			if (readCount != vertexBuffer.length) {
				Vertex[] tmp = new Vertex[readCount];
				for (int i = 0; i < readCount; i++) {
					tmp[i] = vertexBuffer[i];
				}
				
				vertexBuffer = tmp;
				loop = false;
			}
			
			if (m_chunkService.create(vertexBuffer) != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Creating chunks for vertices failed.");
				return false;
			}
			
			if (m_chunkService.put(vertexBuffer) != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Putting vertex data for chunks failed.");
				return false;
			}
			
			for (Vertex v : vertexBuffer)
			{
				System.out.println(v);
			}
		}
		
		return true;
	}
	
	public static class GraphIndex implements Importable, Exportable
	{
		private List<Entry> m_index = new ArrayList<Entry>();
		
		public GraphIndex()
		{
			
		}
		
		public List<Entry> getIndex() {
			return m_index;
		}
		
		public long rebaseNeighborLocalID(final long p_neighborID, final int p_curNodeIndex)
		{
			// find section the vertex (of the neighbor) is in
			long globalVertexIDOffset = 0;
			for (Entry entry : m_index) {
				if (p_neighborID >= globalVertexIDOffset && p_neighborID < globalVertexIDOffset + entry.m_vertexCount) {
					return p_neighborID - globalVertexIDOffset;
				}
				
				globalVertexIDOffset += entry.m_vertexCount;
			}
			
			// out of range ID
			return -1;
		}
		
		public static class Entry implements Importable, Exportable
		{
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
	private void incomingMasterSyncGraphIndex(final LoaderMasterSyncGraphIndexMessage p_message) 
	{
		m_graphIndex = p_message.getIndex();
	}
	
	/**
	 * Handles an incoming LoaderMasterSyncGraphIndex
	 * @param p_message
	 *            the incoming message
	 */
	private void incomingMasterBroadcast(final LoaderMasterBroadcastMessage p_message) 
	{
		m_masterNodeID = p_message.getSource();
	}
}
