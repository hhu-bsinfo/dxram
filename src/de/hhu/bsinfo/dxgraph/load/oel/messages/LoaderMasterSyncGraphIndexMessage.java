package de.hhu.bsinfo.dxgraph.load.oel.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message sent by the master of a distributed ordered edge list loading process.
 * The master sends the slave an index, so they know about the location of vertex ID
 * ranges on other nodes for re-basing the vertex IDs within a neighbor list.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 *
 */
public class LoaderMasterSyncGraphIndexMessage extends AbstractMessage {
	private GraphLoaderOrderedEdgeListMultiNode.GraphIndex m_index = null;
	
	/**
	 * Creates an instance of StatusResponse.
	 * This constructor is used when receiving this message.
	 */
	public LoaderMasterSyncGraphIndexMessage() {
		super();
	}

	/**
	 * Creates an instance of LoaderSlaveSignOn.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public LoaderMasterSyncGraphIndexMessage(final short p_destination, final GraphLoaderOrderedEdgeListMultiNode.GraphIndex p_index) {
		super(p_destination, GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUBTYPE_MASTER_SYNC_GRAPH_INDEX);
	
		m_index = p_index;
	}
	
	/**
	 * Get the graph index.
	 * @return Graph index.
	 */
	public final GraphLoaderOrderedEdgeListMultiNode.GraphIndex getIndex() {
		return m_index;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);	
		exporter.exportObject(m_index);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		m_index = new GraphLoaderOrderedEdgeListMultiNode.GraphIndex();
		importer.importObject(m_index);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return m_index.sizeofObject();
	}
}
