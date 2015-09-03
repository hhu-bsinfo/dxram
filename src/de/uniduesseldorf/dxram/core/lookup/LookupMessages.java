
package de.uniduesseldorf.dxram.core.lookup;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.storage.CIDTreeOptimized;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.AbstractResponse;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Encapsulates messages for the LookupHandler
 * @author Kevin Beinekechun
 *         03.06.2013
 */
public final class LookupMessages {

	// Constants
	public static final byte TYPE = 20;
	public static final byte SUBTYPE_JOIN_REQUEST = 1;
	public static final byte SUBTYPE_JOIN_RESPONSE = 2;
	public static final byte SUBTYPE_INIT_RANGE_REQUEST = 3;
	public static final byte SUBTYPE_INIT_RANGE_RESPONSE = 4;
	public static final byte SUBTYPE_LOOKUP_REQUEST = 5;
	public static final byte SUBTYPE_LOOKUP_RESPONSE = 6;
	public static final byte SUBTYPE_MIGRATE_REQUEST = 7;
	public static final byte SUBTYPE_MIGRATE_RESPONSE = 8;
	public static final byte SUBTYPE_MIGRATE_MESSAGE = 9;
	public static final byte SUBTYPE_MIGRATE_RANGE_REQUEST = 10;
	public static final byte SUBTYPE_MIGRATE_RANGE_RESPONSE = 11;
	public static final byte SUBTYPE_REMOVE_REQUEST = 12;
	public static final byte SUBTYPE_REMOVE_RESPONSE = 13;
	public static final byte SUBTYPE_SEND_BACKUPS_MESSAGE = 14;
	public static final byte SUBTYPE_SEND_SUPERPEERS_MESSAGE = 15;
	public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST = 16;
	public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE = 17;
	public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST = 18;
	public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE = 19;
	public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE = 20;
	public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE = 21;
	public static final byte SUBTYPE_PING_SUPERPEER_MESSAGE = 22;
	public static final byte SUBTYPE_SEARCH_FOR_PEER_REQUEST = 23;
	public static final byte SUBTYPE_SEARCH_FOR_PEER_RESPONSE = 24;
	public static final byte SUBTYPE_PROMOTE_PEER_REQUEST = 25;
	public static final byte SUBTYPE_PROMOTE_PEER_RESPONSE = 26;
	public static final byte SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE = 27;
	public static final byte SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE = 28;
	public static final byte SUBTYPE_START_RECOVERY_MESSAGE = 29;
	public static final byte SUBTYPE_INSERT_ID_REQUEST = 30;
	public static final byte SUBTYPE_INSERT_ID_RESPONSE = 31;
	public static final byte SUBTYPE_GET_CHUNKID_REQUEST = 32;
	public static final byte SUBTYPE_GET_CHUNKID_RESPONSE = 33;
	public static final byte SUBTYPE_GET_MAPPING_COUNT_REQUEST = 34;
	public static final byte SUBTYPE_GET_MAPPING_COUNT_RESPONSE = 35;
	public static final byte SUBTYPE_LOOKUP_REFLECTION_REQUEST = 36;
	public static final byte SUBTYPE_LOOKUP_REFLECTION_RESPONSE = 37;

	// Constructors
	/**
	 * Creates an instance of LookupMessages
	 */
	private LookupMessages() {}

	// Classes
	/**
	 * Join Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class JoinRequest extends AbstractRequest {

		// Attributes
		private short m_newNode;
		private boolean m_nodeIsSuperpeer;

		// Constructors
		/**
		 * Creates an instance of JoinRequest
		 */
		public JoinRequest() {
			super();

			m_newNode = -1;
			m_nodeIsSuperpeer = false;
		}

		/**
		 * Creates an instance of JoinRequest
		 * @param p_destination
		 *            the destination
		 * @param p_newNode
		 *            the NodeID of the new node
		 * @param p_nodeIsSuperpeer
		 *            wether the new node is a superpeer or not
		 */
		public JoinRequest(final short p_destination, final short p_newNode, final boolean p_nodeIsSuperpeer) {
			super(p_destination, TYPE, SUBTYPE_JOIN_REQUEST);

			Contract.checkNotNull(p_newNode, "new LookupNode is null");

			m_newNode = p_newNode;
			m_nodeIsSuperpeer = p_nodeIsSuperpeer;
		}

		// Getters
		/**
		 * Get new node
		 * @return the NodeID
		 */
		public final short getNewNode() {
			return m_newNode;
		}

		/**
		 * Get role of new node
		 * @return true if the new node is a superpeer, false otherwise
		 */
		public final boolean nodeIsSuperpeer() {
			return m_nodeIsSuperpeer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_newNode);
			OutputHelper.writeBoolean(p_buffer, m_nodeIsSuperpeer);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_newNode = InputHelper.readNodeID(p_buffer);
			m_nodeIsSuperpeer = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength() + OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a JoinRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class JoinResponse extends AbstractResponse {

		// Attributes
		private short m_newContactSuperpeer;
		private short m_predecessor;
		private short m_successor;
		private byte[] m_mappings;
		private ArrayList<Short> m_superpeers;
		private ArrayList<Short> m_peers;
		private ArrayList<CIDTreeOptimized> m_trees;

		// Constructors
		/**
		 * Creates an instance of JoinResponse
		 */
		public JoinResponse() {
			super();

			m_newContactSuperpeer = -1;
			m_predecessor = -1;
			m_successor = -1;
			m_mappings = null;
			m_superpeers = null;
			m_peers = null;
			m_trees = null;
		}

		/**
		 * Creates an instance of JoinResponse
		 * @param p_request
		 *            the corresponding JoinRequest
		 * @param p_newContactSuperpeer
		 *            the superpeer that has to be asked next
		 * @param p_predecessor
		 *            the predecessor
		 * @param p_successor
		 *            the successor
		 * @param p_mappings
		 *            the id mappings
		 * @param p_superpeers
		 *            the finger superpeers
		 * @param p_peers
		 *            the peers the superpeer is responsible for
		 * @param p_trees
		 *            the CIDTrees of the peers
		 */
		public JoinResponse(final JoinRequest p_request, final short p_newContactSuperpeer,
				final short p_predecessor, final short p_successor, final byte[] p_mappings,
				final ArrayList<Short> p_superpeers, final ArrayList<Short> p_peers,
				final ArrayList<CIDTreeOptimized> p_trees) {
			super(p_request, SUBTYPE_JOIN_RESPONSE);

			m_newContactSuperpeer = p_newContactSuperpeer;
			m_predecessor = p_predecessor;
			m_successor = p_successor;
			m_superpeers = p_superpeers;
			m_mappings = p_mappings;
			m_peers = p_peers;
			m_trees = p_trees;
		}

		// Getters
		/**
		 * Get new contact superpeer
		 * @return the NodeID
		 */
		public final short getNewContactSuperpeer() {
			return m_newContactSuperpeer;
		}

		/**
		 * Get predecessor
		 * @return the NodeID
		 */
		public final short getPredecessor() {
			return m_predecessor;
		}

		/**
		 * Get successor
		 * @return the NodeID
		 */
		public final short getSuccessor() {
			return m_successor;
		}

		/**
		 * Get mappings
		 * @return the byte array
		 */
		public final byte[] getMappings() {
			return m_mappings;
		}

		/**
		 * Get superpeers
		 * @return the NodeIDs
		 */
		public final ArrayList<Short> getSuperpeers() {
			return m_superpeers;
		}

		/**
		 * Get peers
		 * @return the NodeIDs
		 */
		public final ArrayList<Short> getPeers() {
			return m_peers;
		}

		/**
		 * Get CIDTrees
		 * @return the CIDTrees
		 */
		public final ArrayList<CIDTreeOptimized> getCIDTrees() {
			return m_trees;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_newContactSuperpeer == -1) {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeNodeID(p_buffer, m_predecessor);
				OutputHelper.writeNodeID(p_buffer, m_successor);

				if (m_mappings == null) {
					OutputHelper.writeBoolean(p_buffer, false);
				} else {
					OutputHelper.writeBoolean(p_buffer, true);
					OutputHelper.writeByteArray(p_buffer, m_mappings);
				}

				OutputHelper.writeInt(p_buffer, m_superpeers.size());
				for (short superpeer : m_superpeers) {
					OutputHelper.writeNodeID(p_buffer, superpeer);
				}

				if (m_peers == null) {
					OutputHelper.writeInt(p_buffer, 0);
				} else {
					OutputHelper.writeInt(p_buffer, m_peers.size());
					for (short peer : m_peers) {
						OutputHelper.writeNodeID(p_buffer, peer);
					}
				}

				if (m_trees == null) {
					OutputHelper.writeInt(p_buffer, 0);
				} else {
					OutputHelper.writeInt(p_buffer, m_trees.size());
					for (CIDTreeOptimized tree : m_trees) {
						OutputHelper.writeCIDTree(p_buffer, tree);
					}
				}
			} else {
				OutputHelper.writeBoolean(p_buffer, false);
				OutputHelper.writeNodeID(p_buffer, m_newContactSuperpeer);
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			if (InputHelper.readBoolean(p_buffer)) {
				m_predecessor = InputHelper.readNodeID(p_buffer);
				m_successor = InputHelper.readNodeID(p_buffer);

				if (InputHelper.readBoolean(p_buffer)) {
					m_mappings = InputHelper.readByteArray(p_buffer);
				}

				length = InputHelper.readInt(p_buffer);
				m_superpeers = new ArrayList<Short>();
				for (int i = 0; i < length; i++) {
					m_superpeers.add(InputHelper.readNodeID(p_buffer));
				}

				m_peers = new ArrayList<Short>();
				length = InputHelper.readInt(p_buffer);
				for (int i = 0; i < length; i++) {
					m_peers.add(InputHelper.readNodeID(p_buffer));
				}

				m_trees = new ArrayList<CIDTreeOptimized>();
				length = InputHelper.readInt(p_buffer);
				for (int i = 0; i < length; i++) {
					m_trees.add(InputHelper.readCIDTree(p_buffer));
				}
			} else {
				m_newContactSuperpeer = InputHelper.readNodeID(p_buffer);
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			if (m_newContactSuperpeer == -1) {
				ret = OutputHelper.getBooleanWriteLength() + OutputHelper.getNodeIDWriteLength() * 2;

				ret += OutputHelper.getBooleanWriteLength();
				if (m_mappings != null) {
					ret += OutputHelper.getByteArrayWriteLength(m_mappings.length);
				}

				ret += OutputHelper.getIntWriteLength() + OutputHelper.getNodeIDWriteLength() * m_superpeers.size();

				ret += OutputHelper.getIntWriteLength();
				if (m_peers != null) {
					ret += OutputHelper.getNodeIDWriteLength() * m_peers.size();
				}

				ret += OutputHelper.getIntWriteLength();
				if (m_trees != null) {
					for (CIDTreeOptimized tree : m_trees) {
						ret += OutputHelper.getCIDTreeWriteLength(tree);
					}
				}
			} else {
				ret = OutputHelper.getBooleanWriteLength() + OutputHelper.getNodeIDWriteLength();
			}

			return ret;
		}

	}

	/**
	 * Lookup Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class LookupRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;

		// Constructors
		/**
		 * Creates an instance of LookupRequest
		 */
		public LookupRequest() {
			super();

			m_chunkID = -1;
		}

		/**
		 * Creates an instance of LookupRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            the ChunkID of the requested object
		 */
		public LookupRequest(final short p_destination, final long p_chunkID) {
			super(p_destination, TYPE, SUBTYPE_LOOKUP_REQUEST);

			Contract.checkNotNull(p_chunkID, "no ChunkID given");

			m_chunkID = p_chunkID;
		}

		// Getters
		/**
		 * Get the ChunkID
		 * @return the ChunkID
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Response to a LookupRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class LookupResponse extends AbstractResponse {

		// Attributes
		private Locations m_locations;

		// Constructors
		/**
		 * Creates an instance of LookupResponse
		 */
		public LookupResponse() {
			super();

			m_locations = null;
		}

		/**
		 * Creates an instance of LookupResponse
		 * @param p_request
		 *            the corresponding LookupRequest
		 * @param p_locations
		 *            the primary peer, backup peers and range
		 */
		public LookupResponse(final LookupRequest p_request, final Locations p_locations) {
			super(p_request, SUBTYPE_LOOKUP_RESPONSE);

			m_locations = p_locations;
		}

		// Getters
		/**
		 * Get locations
		 * @return the locations
		 */
		public final Locations getLocations() {
			return m_locations;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_locations == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeLocations(p_buffer, m_locations);
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			if (InputHelper.readBoolean(p_buffer)) {
				m_locations = InputHelper.readLocations(p_buffer);
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			if (m_locations == null) {
				ret = OutputHelper.getBooleanWriteLength();
			} else {
				ret = OutputHelper.getBooleanWriteLength() + OutputHelper.getLocationsWriteLength();
			}

			return ret;
		}

	}

	/**
	 * Migrate Request
	 * @author Kevin Beineke
	 *         03.06.2013
	 */
	public static class MigrateRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;
		private short m_nodeID;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of MigrateRequest
		 */
		public MigrateRequest() {
			super();

			m_chunkID = -1;
			m_nodeID = -1;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of MigrateRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            the object that has to be migrated
		 * @param p_nodeID
		 *            the peer where the object has to be migrated
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public MigrateRequest(final short p_destination, final long p_chunkID, final short p_nodeID,
				final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_MIGRATE_REQUEST);

			m_chunkID = p_chunkID;
			m_nodeID = p_nodeID;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the ChunkID
		 * @return the ID
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Get the NodeID
		 * @return the NodeID
		 */
		public final short getNodeID() {
			return m_nodeID;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
			OutputHelper.writeNodeID(p_buffer, m_nodeID);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
			m_nodeID = InputHelper.readNodeID(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() + OutputHelper.getNodeIDWriteLength()
					+ OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a MigrateRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class MigrateResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of MigrateResponse
		 */
		public MigrateResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of MigrateResponse
		 * @param p_request
		 *            the corresponding MigrateRequest
		 * @param p_success
		 *            whether the migration was successful or not
		 */
		public MigrateResponse(final MigrateRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_MIGRATE_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get the status
		 * @return whether the migration was successful or not
		 */
		public final boolean getStatus() {
			return m_success;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeBoolean(p_buffer, m_success);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_success = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Migrate Message
	 * @author Kevin Beineke
	 *         03.06.2013
	 */
	public static class MigrateMessage extends AbstractMessage {

		// Attributes
		private long m_chunkID;
		private short m_nodeID;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of MigrateMessage
		 */
		public MigrateMessage() {
			super();

			m_chunkID = -1;
			m_nodeID = -1;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of MigrateMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            the object that has to be migrated
		 * @param p_nodeID
		 *            the peer where the object has to be migrated
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public MigrateMessage(final short p_destination, final long p_chunkID, final short p_nodeID,
				final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_MIGRATE_MESSAGE);

			m_chunkID = p_chunkID;
			m_nodeID = p_nodeID;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the ChunkID
		 * @return the ID
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Get the NodeID
		 * @return the NodeID
		 */
		public final short getNodeID() {
			return m_nodeID;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
			OutputHelper.writeNodeID(p_buffer, m_nodeID);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
			m_nodeID = InputHelper.readNodeID(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() + OutputHelper.getNodeIDWriteLength()
					+ OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Migrate Range Request
	 * @author Kevin Beineke
	 *         03.06.2013
	 */
	public static class MigrateRangeRequest extends AbstractRequest {

		// Attributes
		private long m_startChunkID;
		private long m_endChunkID;
		private short m_nodeID;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of MigrateRangeRequest
		 */
		public MigrateRangeRequest() {
			super();

			m_startChunkID = -1;
			m_endChunkID = -1;
			m_nodeID = -1;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of MigrateRangeRequest
		 * @param p_destination
		 *            the destination
		 * @param p_startChunkID
		 *            the first object that has to be migrated
		 * @param p_endChunkID
		 *            the last object that has to be migrated
		 * @param p_nodeID
		 *            the peer where the object has to be migrated
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public MigrateRangeRequest(final short p_destination, final long p_startChunkID, final long p_endChunkID,
				final short p_nodeID, final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_MIGRATE_RANGE_REQUEST);

			m_startChunkID = p_startChunkID;
			m_endChunkID = p_endChunkID;
			m_nodeID = p_nodeID;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the first ChunkID
		 * @return the ID
		 */
		public final long getStartChunkID() {
			return m_startChunkID;
		}

		/**
		 * Get the last ChunkID
		 * @return the ID
		 */
		public final long getEndChunkID() {
			return m_endChunkID;
		}

		/**
		 * Get the NodeID
		 * @return the NodeID
		 */
		public final short getNodeID() {
			return m_nodeID;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_startChunkID);
			OutputHelper.writeChunkID(p_buffer, m_endChunkID);
			OutputHelper.writeNodeID(p_buffer, m_nodeID);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_startChunkID = InputHelper.readChunkID(p_buffer);
			m_endChunkID = InputHelper.readChunkID(p_buffer);
			m_nodeID = InputHelper.readNodeID(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() * 2 + OutputHelper.getNodeIDWriteLength()
					+ OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a MigrateRangeRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class MigrateRangeResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of MigrateRangeResponse
		 */
		public MigrateRangeResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of MigrateRangeResponse
		 * @param p_request
		 *            the corresponding MigrateRangeRequest
		 * @param p_success
		 *            whether the migration was successful or not
		 */
		public MigrateRangeResponse(final MigrateRangeRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_MIGRATE_RANGE_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get the status
		 * @return whether the migration was successful or not
		 */
		public final boolean getStatus() {
			return m_success;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeBoolean(p_buffer, m_success);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_success = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Init Range Request
	 * @author Kevin Beineke
	 *         03.06.2013
	 */
	public static class InitRangeRequest extends AbstractRequest {

		// Attributes
		private long m_startChunkIDOrRangeID;
		private long m_locations;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of InitRangeRequest
		 */
		public InitRangeRequest() {
			super();

			m_startChunkIDOrRangeID = -1;
			m_locations = -1;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of InitRangeRequest
		 * @param p_destination
		 *            the destination
		 * @param p_startChunkID
		 *            the first object
		 * @param p_locations
		 *            the locations (backup peers and own NodeID)
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public InitRangeRequest(final short p_destination, final long p_startChunkID, final long p_locations,
				final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_INIT_RANGE_REQUEST);

			m_startChunkIDOrRangeID = p_startChunkID;
			m_locations = p_locations;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the last ChunkID
		 * @return the ID
		 */
		public final long getStartChunkIDOrRangeID() {
			return m_startChunkIDOrRangeID;
		}

		/**
		 * Get locations
		 * @return the locations
		 */
		public final long getLocations() {
			return m_locations;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_startChunkIDOrRangeID);
			OutputHelper.writeLong(p_buffer, m_locations);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_startChunkIDOrRangeID = InputHelper.readChunkID(p_buffer);
			m_locations = InputHelper.readLong(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() + OutputHelper.getLongWriteLength()
					+ OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a InitRangeRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class InitRangeResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of InitRangeResponse
		 */
		public InitRangeResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of InitRangeResponse
		 * @param p_request
		 *            the corresponding InitRangeRequest
		 * @param p_success
		 *            whether the migration was successful or not
		 */
		public InitRangeResponse(final InitRangeRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_INIT_RANGE_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get the status
		 * @return whether the migration was successful or not
		 */
		public final boolean getStatus() {
			return m_success;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeBoolean(p_buffer, m_success);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_success = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Remove Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class RemoveRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of RemoveRequest
		 */
		public RemoveRequest() {
			super();

			m_chunkID = -1;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of RemoveRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            the ChunkID that has to be removed
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public RemoveRequest(final short p_destination, final long p_chunkID, final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_REQUEST);

			Contract.checkNotNull(p_chunkID, "no ChunkID given");

			m_chunkID = p_chunkID;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the ChunkID
		 * @return the ChunkID
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() + OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a RemoveRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class RemoveResponse extends AbstractResponse {

		// Attributes
		private short[] m_backupSuperpeers;

		// Constructors
		/**
		 * Creates an instance of RemoveResponse
		 */
		public RemoveResponse() {
			super();

			m_backupSuperpeers = null;
		}

		/**
		 * Creates an instance of RemoveResponse
		 * @param p_request
		 *            the corresponding RemoveRequest
		 * @param p_backupSuperpeers
		 *            the backup superpeers
		 */
		public RemoveResponse(final RemoveRequest p_request, final short[] p_backupSuperpeers) {
			super(p_request, SUBTYPE_REMOVE_RESPONSE);

			m_backupSuperpeers = p_backupSuperpeers;
		}

		// Getters
		/**
		 * Get the backup superpeers
		 * @return the backup superpeers
		 */
		public final short[] getBackupSuperpeers() {
			return m_backupSuperpeers;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_backupSuperpeers == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeShortArray(p_buffer, m_backupSuperpeers);
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			if (InputHelper.readBoolean(p_buffer)) {
				m_backupSuperpeers = InputHelper.readShortArray(p_buffer);
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			if (m_backupSuperpeers == null) {
				ret = OutputHelper.getBooleanWriteLength();
			} else {
				ret = OutputHelper.getBooleanWriteLength()
						+ OutputHelper.getShortArrayWriteLength(m_backupSuperpeers.length);
			}

			return ret;
		}

	}

	/**
	 * Ask About Backups Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class AskAboutBackupsRequest extends AbstractRequest {

		// Attributes
		private ArrayList<Short> m_peers;

		// Constructors
		/**
		 * Creates an instance of AskAboutBackupsRequest
		 */
		public AskAboutBackupsRequest() {
			super();

			m_peers = null;
		}

		/**
		 * Creates an instance of AskAboutBackupsRequest
		 * @param p_destination
		 *            the destination
		 * @param p_peers
		 *            all peers for which this superpeer stores backups
		 */
		public AskAboutBackupsRequest(final short p_destination, final ArrayList<Short> p_peers) {
			super(p_destination, TYPE, SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST);

			m_peers = p_peers;
		}

		// Getters
		/**
		 * Get the peers for which the superpeer stores backups
		 * @return the peers
		 */
		public final ArrayList<Short> getPeers() {
			return m_peers;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_peers == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_peers.size());
				for (short peer : m_peers) {
					OutputHelper.writeNodeID(p_buffer, peer);
				}
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			m_peers = new ArrayList<Short>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_peers.add(InputHelper.readNodeID(p_buffer));
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			ret = OutputHelper.getIntWriteLength();
			if (m_peers != null) {
				ret += OutputHelper.getNodeIDWriteLength() * m_peers.size();
			}

			return ret;
		}

	}

	/**
	 * Response to a AskAboutBackupsRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class AskAboutBackupsResponse extends AbstractResponse {

		// Attributes
		private ArrayList<CIDTreeOptimized> m_trees;
		private byte[] m_mappings;

		// Constructors
		/**
		 * Creates an instance of AskAboutBackupsResponse
		 */
		public AskAboutBackupsResponse() {
			super();

			m_trees = null;
			m_mappings = null;
		}

		/**
		 * Creates an instance of AskAboutBackupsResponse
		 * @param p_request
		 *            the corresponding AskAboutBackupsRequest
		 * @param p_trees
		 *            the missing backups
		 * @param p_mappings
		 *            the missing id mappings
		 */
		public AskAboutBackupsResponse(final AskAboutBackupsRequest p_request,
				final ArrayList<CIDTreeOptimized> p_trees,
				final byte[] p_mappings) {
			super(p_request, SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE);

			m_trees = p_trees;
			m_mappings = p_mappings;
		}

		// Getters
		/**
		 * Get the missing backups
		 * @return the CIDTrees
		 */
		public final ArrayList<CIDTreeOptimized> getBackups() {
			return m_trees;
		}

		/**
		 * Get the missing id mappings
		 * @return the byte array
		 */
		public final byte[] getMappings() {
			return m_mappings;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_mappings == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeByteArray(p_buffer, m_mappings);
			}

			if (m_trees == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_trees.size());
				for (CIDTreeOptimized tree : m_trees) {
					OutputHelper.writeCIDTree(p_buffer, tree);
				}
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			if (InputHelper.readBoolean(p_buffer)) {
				m_mappings = InputHelper.readByteArray(p_buffer);
			}

			m_trees = new ArrayList<CIDTreeOptimized>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_trees.add(InputHelper.readCIDTree(p_buffer));
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			ret = OutputHelper.getBooleanWriteLength();
			if (m_mappings != null) {
				ret += OutputHelper.getByteArrayWriteLength(m_mappings.length);
			}

			ret += OutputHelper.getIntWriteLength();
			if (m_trees != null) {
				for (CIDTreeOptimized tree : m_trees) {
					ret += OutputHelper.getCIDTreeWriteLength(tree);
				}
			}

			return ret;
		}

	}

	/**
	 * Ask About Successor Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class AskAboutSuccessorRequest extends AbstractRequest {

		// Constructors
		/**
		 * Creates an instance of AskAboutSuccessorRequest
		 */
		public AskAboutSuccessorRequest() {
			super();
		}

		/**
		 * Creates an instance of AskAboutSuccessorRequest
		 * @param p_destination
		 *            the destination
		 */
		public AskAboutSuccessorRequest(final short p_destination) {
			super(p_destination, TYPE, SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST);
		}

	}

	/**
	 * Response to a AskAboutSuccessorRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class AskAboutSuccessorResponse extends AbstractResponse {

		// Attributes
		private short m_successor;

		// Constructors
		/**
		 * Creates an instance of AskAboutSuccessorResponse
		 */
		public AskAboutSuccessorResponse() {
			super();

			m_successor = -1;
		}

		/**
		 * Creates an instance of AskAboutSuccessorResponse
		 * @param p_request
		 *            the corresponding AskAboutSuccessorRequest
		 * @param p_predecessor
		 *            the predecessor
		 */
		public AskAboutSuccessorResponse(final AskAboutSuccessorRequest p_request, final short p_predecessor) {
			super(p_request, SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE);

			Contract.checkNotNull(p_predecessor, "no predecessor given");

			m_successor = p_predecessor;
		}

		// Getters
		/**
		 * Get the predecessor
		 * @return the NodeID
		 */
		public final short getSuccessor() {
			return m_successor;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_successor);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_successor = InputHelper.readNodeID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength();
		}

	}

	/**
	 * Notify About New Predecessor Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class NotifyAboutNewPredecessorMessage extends AbstractMessage {

		// Attributes
		private short m_newPredecessor;

		// Constructors
		/**
		 * Creates an instance of NotifyAboutNewPredecessorMessage
		 */
		public NotifyAboutNewPredecessorMessage() {
			super();

			m_newPredecessor = -1;
		}

		/**
		 * Creates an instance of NotifyAboutNewPredecessorMessage
		 * @param p_destination
		 *            the destination
		 * @param p_newPredecessor
		 *            the new predecessor
		 */
		public NotifyAboutNewPredecessorMessage(final short p_destination, final short p_newPredecessor) {
			super(p_destination, TYPE, SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE);

			Contract.checkNotNull(p_newPredecessor, "no new predecessor given");

			m_newPredecessor = p_newPredecessor;
		}

		// Getters
		/**
		 * Get the new predecessor
		 * @return the NodeID
		 */
		public final short getNewPredecessor() {
			return m_newPredecessor;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_newPredecessor);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_newPredecessor = InputHelper.readNodeID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength();
		}

	}

	/**
	 * Notify About New Successor Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class NotifyAboutNewSuccessorMessage extends AbstractMessage {

		// Attributes
		private short m_newSuccessor;

		// Constructors
		/**
		 * Creates an instance of NotifyAboutNewSuccessorMessage
		 */
		public NotifyAboutNewSuccessorMessage() {
			super();

			m_newSuccessor = -1;
		}

		/**
		 * Creates an instance of NotifyAboutNewSuccessorMessage
		 * @param p_destination
		 *            the destination
		 * @param p_newSuccessor
		 *            the new successor
		 */
		public NotifyAboutNewSuccessorMessage(final short p_destination, final short p_newSuccessor) {
			super(p_destination, TYPE, SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE);

			Contract.checkNotNull(p_newSuccessor, "no new successor given");

			m_newSuccessor = p_newSuccessor;
		}

		// Getters
		/**
		 * Get new successor
		 * @return the NodeID
		 */
		public final short getNewSuccessor() {
			return m_newSuccessor;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_newSuccessor);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_newSuccessor = InputHelper.readNodeID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength();
		}

	}

	/**
	 * Send Backups Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class SendBackupsMessage extends AbstractMessage {

		// Attributes
		private ArrayList<CIDTreeOptimized> m_trees;
		private byte[] m_mappings;

		// Constructors
		/**
		 * Creates an instance of SendBackupsMessage
		 */
		public SendBackupsMessage() {
			super();

			m_trees = null;
			m_mappings = null;
		}

		/**
		 * Creates an instance of SendBackupsMessage
		 * @param p_destination
		 *            the destination
		 * @param p_mappings
		 *            the id mappings
		 * @param p_trees
		 *            the CIDTrees
		 */
		public SendBackupsMessage(final short p_destination, final byte[] p_mappings,
				final ArrayList<CIDTreeOptimized> p_trees) {
			super(p_destination, TYPE, SUBTYPE_SEND_BACKUPS_MESSAGE);

			m_mappings = p_mappings;
			m_trees = p_trees;
		}

		// Getters
		/**
		 * Get CIDTrees
		 * @return the CIDTrees
		 */
		public final ArrayList<CIDTreeOptimized> getCIDTrees() {
			return m_trees;
		}

		/**
		 * Get id mappings
		 * @return the byte array
		 */
		public final byte[] getMappings() {
			return m_mappings;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_mappings == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeByteArray(p_buffer, m_mappings);
			}

			if (m_trees == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_trees.size());
				for (CIDTreeOptimized tree : m_trees) {
					OutputHelper.writeCIDTree(p_buffer, tree);
				}
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			if (InputHelper.readBoolean(p_buffer)) {
				m_mappings = InputHelper.readByteArray(p_buffer);
			}

			m_trees = new ArrayList<CIDTreeOptimized>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_trees.add(InputHelper.readCIDTree(p_buffer));
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			ret = OutputHelper.getBooleanWriteLength();
			if (m_mappings != null) {
				ret += OutputHelper.getByteArrayWriteLength(m_mappings.length);
			}

			ret += OutputHelper.getIntWriteLength();
			if (m_trees != null) {
				for (CIDTreeOptimized tree : m_trees) {
					ret += OutputHelper.getCIDTreeWriteLength(tree);
				}
			}

			return ret;
		}

	}

	/**
	 * Send Superpeers Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class SendSuperpeersMessage extends AbstractMessage {

		// Attributes
		private ArrayList<Short> m_superpeers;

		// Constructors
		/**
		 * Creates an instance of SendSuperpeersMessage
		 */
		public SendSuperpeersMessage() {
			super();

			m_superpeers = null;
		}

		/**
		 * Creates an instance of SendSuperpeersMessage
		 * @param p_destination
		 *            the destination
		 * @param p_superpeers
		 *            the superpeers
		 */
		public SendSuperpeersMessage(final short p_destination, final ArrayList<Short> p_superpeers) {
			super(p_destination, TYPE, SUBTYPE_SEND_SUPERPEERS_MESSAGE);

			Contract.checkNotNull(p_superpeers, "no superpeers given");

			m_superpeers = p_superpeers;
		}

		// Getters
		/**
		 * Get the superpeers
		 * @return the superpeer array
		 */
		public final ArrayList<Short> getSuperpeers() {
			return m_superpeers;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_superpeers == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_superpeers.size());
				for (short peer : m_superpeers) {
					OutputHelper.writeNodeID(p_buffer, peer);
				}
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			m_superpeers = new ArrayList<Short>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_superpeers.add(InputHelper.readNodeID(p_buffer));
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			ret = OutputHelper.getIntWriteLength();
			if (m_superpeers != null) {
				ret += OutputHelper.getNodeIDWriteLength() * m_superpeers.size();
			}

			return ret;
		}

	}

	/**
	 * Ping Superpeer Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class PingSuperpeerMessage extends AbstractMessage {

		// Constructors
		/**
		 * Creates an instance of PingSuperpeerMessage
		 */
		public PingSuperpeerMessage() {
			super();
		}

		/**
		 * Creates an instance of PingSuperpeerMessage
		 * @param p_destination
		 *            the destination
		 */
		public PingSuperpeerMessage(final short p_destination) {
			super(p_destination, TYPE, SUBTYPE_PING_SUPERPEER_MESSAGE);
		}

	}

	/**
	 * Promote Peer Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class PromotePeerRequest extends AbstractRequest {

		// Attributes
		private short m_predecessor;
		private short m_successor;
		private short m_replacement;
		private byte[] m_mappings;
		private ArrayList<Short> m_superpeers;
		private ArrayList<Short> m_peers;
		private ArrayList<CIDTreeOptimized> m_trees;

		// Constructors
		/**
		 * Creates an instance of PromotePeerRequest
		 */
		public PromotePeerRequest() {
			super();

			m_predecessor = -1;
			m_successor = -1;
			m_replacement = -1;
			m_mappings = null;
			m_superpeers = null;
			m_peers = null;
			m_trees = null;
		}

		/**
		 * Creates an instance of PromotePeerRequest
		 * @param p_destination
		 *            the destination
		 * @param p_predecessor
		 *            the predecessor
		 * @param p_successor
		 *            the successor
		 * @param p_replacement
		 *            the peer that should store p_destination's chunks
		 * @param p_mappings
		 *            the id mappings
		 * @param p_superpeers
		 *            the finger superpeers
		 * @param p_peers
		 *            the peers the superpeer is responsible for
		 * @param p_trees
		 *            the CIDTrees of the peers
		 */
		public PromotePeerRequest(final short p_destination, final short p_predecessor, final short p_successor,
				final short p_replacement, final byte[] p_mappings, final ArrayList<Short> p_superpeers,
				final ArrayList<Short> p_peers, final ArrayList<CIDTreeOptimized> p_trees) {
			super(p_destination, TYPE, SUBTYPE_PROMOTE_PEER_REQUEST);

			m_predecessor = p_predecessor;
			m_successor = p_successor;
			m_superpeers = p_superpeers;
			m_mappings = p_mappings;
			m_replacement = p_replacement;
			m_peers = p_peers;
			m_trees = p_trees;
		}

		// Getters
		/**
		 * Get predecessor
		 * @return the NodeID
		 */
		public final short getPredecessor() {
			return m_predecessor;
		}

		/**
		 * Get successor
		 * @return the NodeID
		 */
		public final short getSuccessor() {
			return m_successor;
		}

		/**
		 * Get replacement
		 * @return the NodeID
		 */
		public final short getReplacement() {
			return m_replacement;
		}

		/**
		 * Get id mappings
		 * @return the byte array
		 */
		public final byte[] getMappings() {
			return m_mappings;
		}

		/**
		 * Get superpeers
		 * @return the NodeIDs
		 */
		public final ArrayList<Short> getSuperpeers() {
			return m_superpeers;
		}

		/**
		 * Get peers
		 * @return the NodeIDs
		 */
		public final ArrayList<Short> getPeers() {
			return m_peers;
		}

		/**
		 * Get CIDTrees
		 * @return the CIDTrees
		 */
		public final ArrayList<CIDTreeOptimized> getCIDTrees() {
			return m_trees;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_predecessor);
			OutputHelper.writeNodeID(p_buffer, m_successor);
			OutputHelper.writeNodeID(p_buffer, m_replacement);

			if (m_mappings == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeByteArray(p_buffer, m_mappings);
			}

			OutputHelper.writeInt(p_buffer, m_superpeers.size());
			for (short superpeer : m_superpeers) {
				OutputHelper.writeNodeID(p_buffer, superpeer);
			}

			if (m_peers == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_peers.size());
				for (short peer : m_peers) {
					OutputHelper.writeNodeID(p_buffer, peer);
				}
			}

			if (m_trees == null) {
				OutputHelper.writeInt(p_buffer, 0);
			} else {
				OutputHelper.writeInt(p_buffer, m_trees.size());
				for (CIDTreeOptimized tree : m_trees) {
					OutputHelper.writeCIDTree(p_buffer, tree);
				}
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			int length;

			m_predecessor = InputHelper.readNodeID(p_buffer);
			m_successor = InputHelper.readNodeID(p_buffer);
			m_replacement = InputHelper.readNodeID(p_buffer);

			if (InputHelper.readBoolean(p_buffer)) {
				m_mappings = InputHelper.readByteArray(p_buffer);
			}

			length = InputHelper.readInt(p_buffer);
			m_superpeers = new ArrayList<Short>();
			for (int i = 0; i < length; i++) {
				m_superpeers.add(InputHelper.readNodeID(p_buffer));
			}

			m_peers = new ArrayList<Short>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_peers.add(InputHelper.readNodeID(p_buffer));
			}

			m_trees = new ArrayList<CIDTreeOptimized>();
			length = InputHelper.readInt(p_buffer);
			for (int i = 0; i < length; i++) {
				m_trees.add(InputHelper.readCIDTree(p_buffer));
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			ret = OutputHelper.getNodeIDWriteLength() * 3;

			ret += OutputHelper.getBooleanWriteLength();
			if (m_mappings != null) {
				ret += OutputHelper.getByteArrayWriteLength(m_mappings.length);
			}

			ret += OutputHelper.getIntWriteLength() + OutputHelper.getNodeIDWriteLength() * m_superpeers.size();

			ret += OutputHelper.getIntWriteLength();
			if (m_peers != null) {
				ret += OutputHelper.getNodeIDWriteLength() * m_peers.size();
			}

			ret += OutputHelper.getIntWriteLength();
			if (m_trees != null) {
				for (CIDTreeOptimized tree : m_trees) {
					ret += OutputHelper.getCIDTreeWriteLength(tree);
				}
			}

			return ret;
		}

	}

	/**
	 * Response to a PromotePeerRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class PromotePeerResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of PromotePeerResponse
		 */
		public PromotePeerResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of PromotePeerResponse
		 * @param p_request
		 *            the corresponding PromotePeerRequest
		 * @param p_success
		 *            whether promoting the peer was successful or not
		 */
		public PromotePeerResponse(final PromotePeerRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_PROMOTE_PEER_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get status
		 * @return whether promoting the peer was successful or not
		 */
		public final boolean getStatus() {
			return m_success;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeBoolean(p_buffer, m_success);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_success = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Delegate Promote Peer Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class DelegatePromotePeerMessage extends AbstractMessage {

		// Attributes
		private short m_hops;

		// Constructors
		/**
		 * Creates an instance of DelegatePromotePeerMessage
		 */
		public DelegatePromotePeerMessage() {
			super();

			m_hops = -1;
		}

		/**
		 * Creates an instance of DelegatePromotePeerMessage
		 * @param p_destination
		 *            the destination
		 * @param p_hops
		 *            the number of hops until now
		 */
		public DelegatePromotePeerMessage(final short p_destination, final short p_hops) {
			super(p_destination, TYPE, SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE);

			m_hops = p_hops;
		}

		// Getters
		/**
		 * Get hops
		 * @return the number of hops
		 */
		public final short getHops() {
			return m_hops;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeShort(p_buffer, m_hops);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_hops = InputHelper.readShort(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getShortWriteLength();
		}

	}

	/**
	 * Search for Peer Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class SearchForPeerRequest extends AbstractRequest {

		// Constructors
		/**
		 * Creates an instance of SearchForPeerRequest
		 */
		public SearchForPeerRequest() {
			super();
		}

		/**
		 * Creates an instance of SearchForPeerRequest
		 * @param p_destination
		 *            the destination
		 */
		public SearchForPeerRequest(final short p_destination) {
			super(p_destination, TYPE, SUBTYPE_SEARCH_FOR_PEER_REQUEST);
		}

	}

	/**
	 * Response to a SearchForPeerRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class SearchForPeerResponse extends AbstractResponse {

		// Attributes
		private short m_peer;

		// Constructors
		/**
		 * Creates an instance of SearchForPeerResponse
		 */
		public SearchForPeerResponse() {
			super();

			m_peer = -1;
		}

		/**
		 * Creates an instance of SearchForPeerResponse
		 * @param p_request
		 *            the corresponding SearchForPeerRequest
		 * @param p_peer
		 *            the peer that can be promoted
		 */
		public SearchForPeerResponse(final SearchForPeerRequest p_request, final short p_peer) {
			super(p_request, SUBTYPE_SEARCH_FOR_PEER_RESPONSE);

			m_peer = p_peer;
		}

		// Getters
		/**
		 * Get peer
		 * @return the NodeID
		 */
		public final short getPeer() {
			return m_peer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_peer);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_peer = InputHelper.readNodeID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength();
		}

	}

	/**
	 * Notify About Failed Peer Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class NotifyAboutFailedPeerMessage extends AbstractMessage {

		// Attributes
		private short m_failedPeer;

		// Constructors
		/**
		 * Creates an instance of NotifyAboutFailedPeerMessage
		 */
		public NotifyAboutFailedPeerMessage() {
			super();

			m_failedPeer = -1;
		}

		/**
		 * Creates an instance of NotifyAboutFailedPeerMessage
		 * @param p_destination
		 *            the destination
		 * @param p_failedPeer
		 *            the failed peer
		 */
		public NotifyAboutFailedPeerMessage(final short p_destination, final short p_failedPeer) {
			super(p_destination, TYPE, SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE);

			Contract.checkNotNull(p_failedPeer, "no failed peer given");

			m_failedPeer = p_failedPeer;
		}

		// Getters
		/**
		 * Get the failed peer
		 * @return the NodeID
		 */
		public final short getFailedPeer() {
			return m_failedPeer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_failedPeer);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_failedPeer = InputHelper.readNodeID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength();
		}

	}

	/**
	 * Start Recovery Message
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class StartRecoveryMessage extends AbstractMessage {

		// Attributes
		private short m_failedPeer;
		private long m_beginOfRange;

		// Constructors
		/**
		 * Creates an instance of StartRecoveryMessage
		 */
		public StartRecoveryMessage() {
			super();

			m_failedPeer = -1;
			m_beginOfRange = -1;
		}

		/**
		 * Creates an instance of StartRecoveryMessage
		 * @param p_destination
		 *            the destination
		 * @param p_failedPeer
		 *            the failed peer
		 * @param p_beginOfRange
		 *            the beginning of the range that has to be recovered
		 */
		public StartRecoveryMessage(final short p_destination, final short p_failedPeer, final int p_beginOfRange) {
			super(p_destination, TYPE, SUBTYPE_START_RECOVERY_MESSAGE);

			Contract.checkNotNull(p_failedPeer, "no failed peer given");

			m_failedPeer = p_failedPeer;
			m_beginOfRange = p_beginOfRange;
		}

		// Getters
		/**
		 * Get the failed peer
		 * @return the NodeID
		 */
		public final short getFailedPeer() {
			return m_failedPeer;
		}

		/**
		 * Get the beginning of range
		 * @return the beginning of the range that has to be recovered
		 */
		public final long getBeginOfRange() {
			return m_beginOfRange;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_failedPeer);
			OutputHelper.writeChunkID(p_buffer, m_beginOfRange);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_failedPeer = InputHelper.readNodeID(p_buffer);
			m_beginOfRange = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength() + OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Request for storing an id to ChunkID mapping on a remote node
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static class InsertIDRequest extends AbstractRequest {

		// Attributes
		private int m_id;
		private long m_chunkID;
		private boolean m_isBackup;

		// Constructors
		/**
		 * Creates an instance of InsertIDRequest
		 */
		public InsertIDRequest() {
			super();

			m_id = -1;
			m_chunkID = ChunkID.INVALID_ID;
			m_isBackup = false;
		}

		/**
		 * Creates an instance of InsertIDRequest
		 * @param p_destination
		 *            the destination
		 * @param p_id
		 *            the id to store
		 * @param p_chunkID
		 *            the ChunkID to store
		 * @param p_isBackup
		 *            whether this is a backup message or not
		 */
		public InsertIDRequest(final short p_destination, final int p_id, final long p_chunkID,
				final boolean p_isBackup) {
			super(p_destination, TYPE, SUBTYPE_INSERT_ID_REQUEST);

			m_id = p_id;
			m_chunkID = p_chunkID;
			m_isBackup = p_isBackup;
		}

		// Getters
		/**
		 * Get the id to store
		 * @return the id to store
		 */
		public final int getID() {
			return m_id;
		}

		/**
		 * Get the ChunkID to store
		 * @return the ChunkID to store
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Returns whether this is a backup message or not
		 * @return whether this is a backup message or not
		 */
		public final boolean isBackup() {
			return m_isBackup;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeInt(p_buffer, m_id);
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
			OutputHelper.writeBoolean(p_buffer, m_isBackup);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_id = InputHelper.readInt(p_buffer);
			m_chunkID = InputHelper.readChunkID(p_buffer);
			m_isBackup = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getIntWriteLength() + OutputHelper.getChunkIDWriteLength()
					+ OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a InsertIDRequest
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static class InsertIDResponse extends AbstractResponse {

		// Attributes
		private short[] m_backupSuperpeers;

		// Constructors
		/**
		 * Creates an instance of InsertIDResponse
		 */
		public InsertIDResponse() {
			super();

			m_backupSuperpeers = null;
		}

		/**
		 * Creates an instance of InsertIDResponse
		 * @param p_request
		 *            the request
		 * @param p_backupSuperpeers
		 *            the backup superpeers
		 */
		public InsertIDResponse(final InsertIDRequest p_request, final short[] p_backupSuperpeers) {
			super(p_request, SUBTYPE_INSERT_ID_RESPONSE);

			m_backupSuperpeers = p_backupSuperpeers;
		}

		// Getters
		/**
		 * Get the backup superpeers
		 * @return the backup superpeers
		 */
		public final short[] getBackupSuperpeers() {
			return m_backupSuperpeers;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_backupSuperpeers == null) {
				OutputHelper.writeBoolean(p_buffer, false);
			} else {
				OutputHelper.writeBoolean(p_buffer, true);
				OutputHelper.writeShortArray(p_buffer, m_backupSuperpeers);
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			if (InputHelper.readBoolean(p_buffer)) {
				m_backupSuperpeers = InputHelper.readShortArray(p_buffer);
			}
		}

		@Override
		protected final int getPayloadLength() {
			int ret;

			if (m_backupSuperpeers == null) {
				ret = OutputHelper.getBooleanWriteLength();
			} else {
				ret = OutputHelper.getBooleanWriteLength()
						+ OutputHelper.getShortArrayWriteLength(m_backupSuperpeers.length);
			}

			return ret;
		}

	}

	/**
	 * Request for getting the ChunkID to corresponding id on a remote node
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static class GetChunkIDRequest extends AbstractRequest {

		// Attributes
		private int m_id;

		// Constructors
		/**
		 * Creates an instance of GetChunkIDRequest
		 */
		public GetChunkIDRequest() {
			super();

			m_id = -1;
		}

		/**
		 * Creates an instance of GetChunkIDRequest
		 * @param p_destination
		 *            the destination
		 * @param p_id
		 *            the id
		 */
		public GetChunkIDRequest(final short p_destination, final int p_id) {
			super(p_destination, TYPE, SUBTYPE_GET_CHUNKID_REQUEST);

			m_id = p_id;
		}

		// Getters
		/**
		 * Get the id to store
		 * @return the id to store
		 */
		public final int getID() {
			return m_id;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeInt(p_buffer, m_id);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_id = InputHelper.readInt(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getIntWriteLength();
		}

	}

	/**
	 * Response to a GetChunkIDRequest
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static class GetChunkIDResponse extends AbstractResponse {

		// Attributes
		private long m_chunkID;

		// Constructors
		/**
		 * Creates an instance of GetChunkIDResponse
		 */
		public GetChunkIDResponse() {
			super();

			m_chunkID = ChunkID.INVALID_ID;
		}

		/**
		 * Creates an instance of GetChunkIDResponse
		 * @param p_request
		 *            the request
		 * @param p_chunkID
		 *            the ChunkID
		 */
		public GetChunkIDResponse(final GetChunkIDRequest p_request, final long p_chunkID) {
			super(p_request, SUBTYPE_GET_CHUNKID_RESPONSE);

			m_chunkID = p_chunkID;
		}

		// Getters
		/**
		 * Get the ChunkID
		 * @return the ChunkID
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Request for getting the number of mappings
	 * @author klein 26.03.2015
	 */
	public static class GetMappingCountRequest extends AbstractRequest {

		// Constructors
		/**
		 * Creates an instance of GetMappingCountRequest
		 */
		public GetMappingCountRequest() {
			super();
		}

		/**
		 * Creates an instance of GetMappingCountRequest
		 * @param p_destination
		 *            the destination
		 */
		public GetMappingCountRequest(final short p_destination) {
			super(p_destination, TYPE, SUBTYPE_GET_MAPPING_COUNT_REQUEST);
		}

	}

	/**
	 * Response to a GetMappingCountRequest
	 * @author klein 26.03.2015
	 */
	public static class GetMappingCountResponse extends AbstractResponse {

		// Attributes
		private long m_count;

		// Constructors
		/**
		 * Creates an instance of GetMappingCountResponse
		 */
		public GetMappingCountResponse() {
			super();

			m_count = 0;
		}

		/**
		 * Creates an instance of GetMappingCountResponse
		 * @param p_request
		 *            the request
		 * @param p_count
		 *            the count
		 */
		public GetMappingCountResponse(final GetMappingCountRequest p_request, final long p_count) {
			super(p_request, SUBTYPE_GET_MAPPING_COUNT_RESPONSE);

			m_count = p_count;
		}

		// Getters
		/**
		 * Get the count
		 * @return the count
		 */
		public final long getCount() {
			return m_count;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeLong(p_buffer, m_count);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_count = InputHelper.readLong(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getLongWriteLength();
		}

	}

	/**
	 * Request for command
	 * @author Michael Schoettner 20.8.2015
	 */
	public static class LookupReflectionRequest extends AbstractRequest {

		// Attributes
		private String m_cmd;

		// Constructors
		/**
		 * Creates an instance of CommandRequest
		 */
		public LookupReflectionRequest() {
			super();
			m_cmd = null;
		}

		/**
		 * Creates an instance of CommandRequest
		 * @param p_destination
		 *            the destination
		 * @param p_cmd
		 *            the command
		 */
		public LookupReflectionRequest(final short p_destination, final String p_cmd) {
			super(p_destination, TYPE, SUBTYPE_LOOKUP_REFLECTION_REQUEST);
			Contract.checkNotNull(p_cmd, "error: no argument given");
			m_cmd = p_cmd;
		}

		
		/**
		 * Get the command
		 * @return the command
		 */
		public final String getArgument() {
			return m_cmd;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeString(p_buffer, m_cmd); 
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_cmd = InputHelper.readString(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getStringsWriteLength(m_cmd);
		}

	}

	/**
	 * Response to a CommandRequest
	 * @author Florian Klein 05.07.2014
	 */
	public static class LookupReflectionResponse extends AbstractResponse {

		// Attributes
		private String m_answer;

		// Constructors
		/**
		 * Creates an instance of CommpandResponse
		 */
		public LookupReflectionResponse() {
			super();

			m_answer = null;
		}

		/**
		 * Creates an instance of CommandResponse
		 * @param p_request
		 *            the corresponding CommandRequest
		 * @param p_answer
		 *            the answer
		 */
		public LookupReflectionResponse(final LookupReflectionRequest p_request, final String p_answer) {
			super(p_request, SUBTYPE_LOOKUP_REFLECTION_RESPONSE);

			m_answer = p_answer;
		}

		// Getters
		/**
		 * Get the answer
		 * @return the answer
		 */
		public final String getAnswer() {
			return m_answer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeString(p_buffer, m_answer);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_answer = InputHelper.readString(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getStringsWriteLength(m_answer);
		}

	}

}
