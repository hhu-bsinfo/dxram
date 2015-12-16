package de.uniduesseldorf.dxram.core.recovery;

import de.uniduesseldorf.dxram.core.chunk.RecoveryMessages;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.mem.Chunk;

import de.uniduesseldorf.utils.Contract;

public class RecoveryService {
	// Recovery Messages
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoveryMessages.RecoverMessage.class);
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoveryMessages.RecoverBackupRangeRequest.class);
	m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoveryMessages.RecoverBackupRangeResponse.class);

	@Override
	public void putRecoveredChunks(final Chunk[] p_chunks) throws DXRAMException {
		Contract.checkNotNull(p_chunks, "no chunks given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			putForeignChunks(p_chunks);
		}
	}
}
