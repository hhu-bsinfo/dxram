package de.hhu.bsinfo.dxgraph.load.oel.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message every peer that participates as a slave in the graph loading process
 * for loading a distributed ordered edge list sends to the master to indicate
 * it is ready. The master can construct the full index using the provided entries.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 *
 */
public class LoaderSlaveSignOnMessage extends AbstractMessage {
	private GraphLoaderOrderedEdgeListMultiNode.GraphIndex.Entry m_indexEntry = null;
	
	/**
	 * Creates an instance of LoaderSlaveSignOn.
	 * This constructor is used when receiving this message.
	 */
	public LoaderSlaveSignOnMessage() {
		super();
	}

	/**
	 * Creates an instance of LoaderSlaveSignOn.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_indexEntry The entry for the graph index of this slave.
	 */
	public LoaderSlaveSignOnMessage(final short p_destination, final GraphLoaderOrderedEdgeListMultiNode.GraphIndex.Entry p_indexEntry) {
		super(p_destination, GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUBTYPE_SLAVE_SIGN_ON);
	
		m_indexEntry = p_indexEntry;
	}
	
	/**
	 * Get the entry for the graph index of the slave node.
	 * @return graph index entry.
	 */
	public GraphLoaderOrderedEdgeListMultiNode.GraphIndex.Entry getIndexEntry() {
		return m_indexEntry;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);	
		exporter.exportObject(m_indexEntry);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		m_indexEntry = new GraphLoaderOrderedEdgeListMultiNode.GraphIndex.Entry();
		importer.importObject(m_indexEntry);
	}

	@Override
	protected final int getPayloadLength() {
		return m_indexEntry.sizeofObject();
	}
}
