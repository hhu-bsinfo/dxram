/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationRequest;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationResponse;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.LogAnonMessage;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class LogService extends AbstractDXRAMService<LogServiceConfig> implements MessageReceiver {
    // component dependencies
    private NetworkComponent m_network;
    private LogComponent m_log;

    /**
     * Constructor
     */
    public LogService() {
        super("log", LogServiceConfig.class);
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization() {
        return m_log.getCurrentUtilization();
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization(final short p_nid) {
        GetUtilizationRequest request = new GetUtilizationRequest(p_nid);

        try {
            m_network.sendSync(request);
        } catch (NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending GetUtilizationRequest failed", e);
            // #endif /* LOGGER >= ERROR */
        }

        GetUtilizationResponse response = (GetUtilizationResponse) request.getResponse();
        return response.getUtilization();
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOG_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LogMessages.SUBTYPE_LOG_MESSAGE:
                        incomingLogMessage((LogMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_LOG_ANON_MESSAGE:
                        incomingLogBufferMessage((LogAnonMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_REMOVE_MESSAGE:
                        incomingRemoveMessage((RemoveMessage) p_message);
                        break;
                    case LogMessages.SUBTYPE_INIT_BACKUP_RANGE_REQUEST:
                        incomingInitBackupRangeRequest((InitBackupRangeRequest) p_message);
                        break;
                    case LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_REQUEST:
                        incomingInitRecoveredBackupRangeRequest((InitRecoveredBackupRangeRequest) p_message);
                        break;
                    case LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST:
                        incomingGetUtilizationRequest((GetUtilizationRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Handles an incoming GetUtilizationRequest
     * Method is used in js script.
     *
     * @param p_request
     *         the GetUtilizationRequest
     */
    @SuppressWarnings("WeakerAccess")
    public void incomingGetUtilizationRequest(final GetUtilizationRequest p_request) {

        try {
            m_network.sendMessage(new GetUtilizationResponse(p_request, getCurrentUtilization()));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not answer GetUtilizationRequest", e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Handles an incoming LogMessage
     *
     * @param p_message
     *         the LogMessage
     */
    private void incomingLogMessage(final LogMessage p_message) {
        m_log.incomingLogChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming LogAnonMessage
     *
     * @param p_message
     *         the LogAnonMessage
     */
    private void incomingLogBufferMessage(final LogAnonMessage p_message) {
        m_log.incomingLogChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming RemoveMessage
     *
     * @param p_message
     *         the RemoveMessage
     */
    private void incomingRemoveMessage(final RemoveMessage p_message) {
        m_log.incomingRemoveChunks(p_message.getMessageBuffer(), p_message.getSource());
    }

    /**
     * Handles an incoming InitBackupRangeRequest
     *
     * @param p_request
     *     the InitBackupRangeRequest
     */
    private void incomingInitBackupRangeRequest(final InitBackupRangeRequest p_request) {
        boolean res;

        res = m_log.incomingInitBackupRange(p_request.getRangeID(), p_request.getSource());

        try {
            m_network.sendMessage(new InitBackupRangeResponse(p_request, res));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not acknowledge initialization of backup range", e);
            // #endif /* LOGGER >= ERROR */

        }
    }

    /**
     * Handles an incoming InitBackupRangeRequest
     *
     * @param p_request
     *     the InitBackupRangeRequest
     */
    private void incomingInitRecoveredBackupRangeRequest(final InitRecoveredBackupRangeRequest p_request) {
        boolean res;

        res = m_log.incomingInitRecoveredBackupRange(p_request.getRangeID(), p_request.getSource(), p_request.getOriginalRangeID(), p_request.getOriginalOwner(),
                p_request.isNewBackupRange());

        try {
            m_network.sendMessage(new InitRecoveredBackupRangeResponse(p_request, res));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not acknowledge initialization of backup range", e);
            // #endif /* LOGGER >= ERROR */

        }
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_ANON_MESSAGE, LogMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, RemoveMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST, GetUtilizationRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_RESPONSE, GetUtilizationResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_ANON_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_BACKUP_RANGE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST, this);
    }
}
