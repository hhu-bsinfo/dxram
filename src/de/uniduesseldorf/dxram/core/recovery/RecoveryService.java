package de.uniduesseldorf.dxram.core.recovery;

import de.uniduesseldorf.dxram.core.chunk.RecoveryMessages;

public class RecoveryService {
	// Recovery Messages
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoveryMessages.RecoverMessage.class);
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoveryMessages.RecoverBackupRangeRequest.class);
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoveryMessages.RecoverBackupRangeResponse.class);
}
