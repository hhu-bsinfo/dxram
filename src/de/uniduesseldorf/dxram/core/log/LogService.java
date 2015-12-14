package de.uniduesseldorf.dxram.core.log;

import de.uniduesseldorf.dxram.core.chunk.LogMessages;

public class LogService {
	// Log Messages
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessages.LogMessage.class);
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_REMOVE_MESSAGE, LogMessages.RemoveMessage.class);
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_INIT_REQUEST, LogMessages.InitRequest.class);
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_INIT_RESPONSE, LogMessages.InitResponse.class);
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_COMMAND_REQUEST, LogMessages.LogCommandRequest.class);
	m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_COMMAND_RESPONSE, LogMessages.LogCommandResponse.class);



}
