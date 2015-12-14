package de.uniduesseldorf.dxram.core.lookup;

public class LookupService {
	// Lookup Messages
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_JOIN_REQUEST, LookupMessages.JoinRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_JOIN_RESPONSE, LookupMessages.JoinResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, LookupMessages.InitRangeRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, LookupMessages.InitRangeResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REQUEST, LookupMessages.LookupRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_RESPONSE, LookupMessages.LookupResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_REQUEST, LookupMessages.GetBackupRangesRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE, LookupMessages.GetBackupRangesResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE, LookupMessages.UpdateAllMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_REQUEST, LookupMessages.MigrateRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, LookupMessages.MigrateResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_MESSAGE, LookupMessages.MigrateMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, LookupMessages.MigrateRangeRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, LookupMessages.MigrateRangeResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_REMOVE_REQUEST, LookupMessages.RemoveRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_REMOVE_RESPONSE, LookupMessages.RemoveResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, LookupMessages.SendBackupsMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, LookupMessages.SendSuperpeersMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, LookupMessages.AskAboutBackupsRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, LookupMessages.AskAboutBackupsResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, LookupMessages.AskAboutSuccessorRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, LookupMessages.AskAboutSuccessorResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE,
			LookupMessages.NotifyAboutNewPredecessorMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, LookupMessages.NotifyAboutNewSuccessorMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, LookupMessages.PingSuperpeerMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST, LookupMessages.SearchForPeerRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE, LookupMessages.SearchForPeerResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST, LookupMessages.PromotePeerRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE, LookupMessages.PromotePeerResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE, LookupMessages.DelegatePromotePeerMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, LookupMessages.NotifyAboutFailedPeerMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, LookupMessages.StartRecoveryMessage.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INSERT_ID_REQUEST, LookupMessages.InsertIDRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INSERT_ID_RESPONSE, LookupMessages.InsertIDResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST, LookupMessages.GetChunkIDRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE, LookupMessages.GetChunkIDResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_REQUEST, LookupMessages.GetMappingCountRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_RESPONSE, LookupMessages.GetMappingCountResponse.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST, LookupMessages.LookupReflectionRequest.class);
	m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE, LookupMessages.LookupReflectionResponse.class);


}
