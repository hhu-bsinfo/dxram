/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.logger.messages.LoggerMessages;
import de.hhu.bsinfo.dxram.logger.messages.SetLogLevelMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Service to allow the application to use the same logger as DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.02.2016
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoggerService extends Service<ModuleConfig> implements MessageReceiver {
    // component dependencies
    private NetworkComponent m_network;
    private BootComponent m_boot;

    /**
     * Set the log level for the logger.
     * Method is used in js script.
     *
     * @param p_logLevel
     *         Log level to set.
     */
    @SuppressWarnings("WeakerAccess")
    public static void setLogLevel(final String p_logLevel) {
        setLogLevel(Level.getLevel(p_logLevel.toUpperCase()));
    }

    /**
     * Set the log level for the logger.
     * Method is used in js script.
     *
     * @param p_logLevel
     *         Log level to set.
     */
    @SuppressWarnings("WeakerAccess")
    public static void setLogLevel(final Level p_logLevel) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(p_logLevel);
        // This causes all Loggers to refetch information from their LoggerConfig
        ctx.updateLoggers();
    }

    /**
     * Handles an incoming SetLogLevelMessage
     *
     * @param p_message
     *         the SetLogLevelMessage
     */
    private static void incomingSetLogLevelMessage(final SetLogLevelMessage p_message) {
        setLogLevel(p_message.getLogLevel());
    }

    /**
     * Set the log level for the logger on another node
     *
     * @param p_logLevel
     *         Log level to set.
     * @param p_nodeId
     *         Id of the node to change the log level on
     */
    public void setLogLevel(final String p_logLevel, final Short p_nodeId) {
        setLogLevel(Level.getLevel(p_logLevel.toUpperCase()), p_nodeId);
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOGGER_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE:
                        incomingSetLogLevelMessage((SetLogLevelMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_boot = p_componentAccessor.getComponent(BootComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_network.registerMessageType(DXRAMMessageTypes.LOGGER_MESSAGES_TYPE,
                LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE, SetLogLevelMessage.class);

        m_network.register(DXRAMMessageTypes.LOGGER_MESSAGES_TYPE, LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE, this);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Set the log level for the logger on another node
     *
     * @param p_logLevel
     *         Log level to set.
     * @param p_nodeId
     *         Id of the node to change the log level on
     */
    private void setLogLevel(final Level p_logLevel, final Short p_nodeId) {
        if (m_boot.getNodeId() == p_nodeId) {
            setLogLevel(p_logLevel);
        } else {
            SetLogLevelMessage message = new SetLogLevelMessage(p_nodeId, p_logLevel.name());
            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {

                LOGGER.error("Setting log level of node 0x%X failed: %s", p_nodeId, e);

            }
        }
    }
}
