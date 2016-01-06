package de.uniduesseldorf.dxram.core.lookup;

import java.util.List;

import de.uniduesseldorf.dxram.core.backup.BackupRange;
import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.lookup.messages.AskAboutBackupsRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.AskAboutBackupsResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.AskAboutSuccessorRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.AskAboutSuccessorResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.DelegatePromotePeerMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.GetBackupRangesRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.GetBackupRangesResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.GetChunkIDRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.GetChunkIDResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.GetMappingCountRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.GetMappingCountResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.InitRangeRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.InitRangeResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.InsertIDRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.InsertIDResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.JoinRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.JoinResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.LookupMessages;
import de.uniduesseldorf.dxram.core.lookup.messages.LookupReflectionRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.LookupReflectionResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.LookupRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.LookupResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.MigrateMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.MigrateRangeRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.MigrateRangeResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.MigrateRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.MigrateResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.NotifyAboutFailedPeerMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.NotifyAboutNewPredecessorMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.NotifyAboutNewSuccessorMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.PingSuperpeerMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.PromotePeerRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.PromotePeerResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.RemoveRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.RemoveResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.SearchForPeerRequest;
import de.uniduesseldorf.dxram.core.lookup.messages.SearchForPeerResponse;
import de.uniduesseldorf.dxram.core.lookup.messages.SendBackupsMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.SendSuperpeersMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.StartRecoveryMessage;
import de.uniduesseldorf.dxram.core.lookup.messages.UpdateAllMessage;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;
import de.uniduesseldorf.utils.config.Configuration;

public abstract class LookupComponent extends DXRAMComponent {
	
	public static final String COMPONENT_IDENTIFIER = "Lookup";
	
	public LookupComponent(int p_priorityInit, int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Get the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @return the corresponding NodeID or null if node ID not available.
	 */
	public abstract Locations get(long p_chunkID);

	/**
	 * Returns all backup ranges (RangeID + backup peers) for given node
	 * @param p_nodeID
	 *            the NodeID
	 * @return all backup ranges for given node
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	public abstract BackupRange[] getAllBackupRanges(short p_nodeID);

	/**
	 * Returns all backup ranges (RangeID + backup peers) for given node
	 * @param p_owner
	 *            the NodeID of the recovered peer
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	public abstract void updateAllAfterRecovery(short p_owner);

	/**
	 * Store migration in meta-data management
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	public abstract void migrate(long p_chunkID, short p_nodeID);

	/**
	 * Store migration of ID range in meta-data management
	 * @param p_startCID
	 *            the first ID
	 * @param p_endCID
	 *            the last ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	public abstract void migrateRange(long p_startCID, long p_endCID, short p_nodeID);

	/**
	 * Store migration in meta-data management; Concerns not created chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	public abstract void migrateNotCreatedChunk(long p_chunkID, short p_nodeID);

	/**
	 * Store migration in meta-data management; Concerns own chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	public abstract void migrateOwnChunk(long p_chunkID, short p_nodeID);

	/**
	 * Initializes the given ID range in CIDTree
	 * @param p_firstChunkIDOrRangeID
	 *            the last ID
	 * @param p_locations
	 *            the NodeID of creator and backup nodes
	 * @throws LookupException
	 *             if the initialization could not be executed
	 */
	public abstract void initRange(long p_firstChunkIDOrRangeID, Locations p_locations);

	/**
	 * Remove the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @throws LookupException
	 *             if the NodeID could not be removed
	 */
	public abstract void remove(long p_chunkID);

	/**
	 * Remove the corresponding NodeIDs for the given IDs
	 * @param p_chunkIDs
	 *            the IDs
	 * @throws LookupException
	 *             if the NodeIDs could not be removed
	 */
	public abstract void remove(long[] p_chunkIDs);

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws LookupException
	 *             if the id could not be inserted
	 */
	public abstract void insertID(int p_id, long p_chunkID);

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @throws LookupException
	 *             if the id could not be found
	 * @return the ChunkID
	 */
	public abstract long getChunkID(int p_id);

	/**
	 * Return the number of identifier mappings
	 * @return the number of mappings
	 * @throws LookupException
	 *             if mapping count could not be gotten
	 */
	public abstract long getMappingCount();

	/**
	 * Invalidates the cache entry for given ChunkIDs
	 * @param p_chunkIDs
	 *            the IDs
	 */
	public abstract void invalidate(final long... p_chunkIDs);

	/**
	 * Invalidates the cache entry for given ChunkID range
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 */
	public abstract void invalidate(final long p_startCID, final long p_endCID);

	/**
	 * Returns if given node is still available as a peer
	 * @param p_creator
	 *            the creator's NodeID
	 * @return true if given node is available and a peer, false otherwise
	 */
	public abstract boolean creatorAvailable(final short p_creator);

	/**
	 * Verifies if there is another node in the superpeer overlay
	 * @return true if there is another node, false otherwise
	 */
	public abstract boolean isLastSuperpeer();

	/**
	 * Returns a list with all superpeers
	 * @return all superpeers
	 */
	public abstract List<Short> getSuperpeers();

	/**
	 * Checks if all superpeers are known
	 * @return true if all superpeers are known, false otherwise
	 */
	public abstract boolean overlayIsStable();
	
	@Override
	protected void registerConfigurationValuesComponent(Configuration p_configuration) {
		p_configuration.registerConfigurationEntries(LookupConfigurationValues.CONFIGURATION_ENTRIES);
	}
	
	@Override
	protected boolean initComponent(final Configuration p_configuration)
	{
		registerNetworkMessages();
		return true;
	}
	
	private void registerNetworkMessages() {
		NetworkComponent network = getDependantComponent(NetworkComponent.COMPONENT_IDENTIFIER);
		
		// Lookup Messages
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST, JoinRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_JOIN_RESPONSE, JoinResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, InitRangeRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, InitRangeResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_REQUEST, LookupRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_RESPONSE, LookupResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_REQUEST, GetBackupRangesRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE, GetBackupRangesResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE, UpdateAllMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_REQUEST, MigrateRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, MigrateResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_MESSAGE, MigrateMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, MigrateRangeRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, MigrateRangeResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_REQUEST, RemoveRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_RESPONSE, RemoveResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, SendBackupsMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, SendSuperpeersMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, AskAboutBackupsRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, AskAboutBackupsResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, AskAboutSuccessorRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, AskAboutSuccessorResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE, NotifyAboutNewPredecessorMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, NotifyAboutNewSuccessorMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, PingSuperpeerMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST, SearchForPeerRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE, SearchForPeerResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST, PromotePeerRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE, PromotePeerResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE, DelegatePromotePeerMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, NotifyAboutFailedPeerMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, StartRecoveryMessage.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_ID_REQUEST, InsertIDRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_ID_RESPONSE, InsertIDResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST, GetChunkIDRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE, GetChunkIDResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_REQUEST, GetMappingCountRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_RESPONSE, GetMappingCountResponse.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST, LookupReflectionRequest.class);
		network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE, LookupReflectionResponse.class);

	}
}
