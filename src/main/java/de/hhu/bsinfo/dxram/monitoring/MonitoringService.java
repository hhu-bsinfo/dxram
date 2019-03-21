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

package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.monitoring.messages.*;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Monitoring Service
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class MonitoringService extends Service<ModuleConfig> implements MessageReceiver {
    private boolean m_peerIsSuperpeer;

    @Dependency
    private BootComponent m_boot;

    @Dependency
    private MonitoringComponent m_monitor;

    @Dependency
    private NetworkComponent m_network;

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_peerIsSuperpeer = m_boot.getNodeRole() == NodeRole.SUPERPEER;

        registerMessageReceiver();
        registerMonitorMessages();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        unregisterMessageReceiver();
        return true;
    }

    /**
     * Registers class as receiver for monitoring messages.
     */
    private void registerMessageReceiver() {
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO,
                this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA,
                this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE,
                this);
    }

    /**
     * Unregisters class as monitornig message receiver.
     */
    private void unregisterMessageReceiver() {
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA,
                this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE,
                this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO,
                this);
    }

    /**
     * Registers monitoring message types.
     */
    private void registerMonitorMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, MonitoringDataRequest.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                        MonitoringMessages.SUBTYPE_MONITORING_DATA_RESPONSE, MonitoringDataResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_DATA, MonitoringDataMessage.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                        MonitoringMessages.SUBTYPE_MONITORING_PROPOSE, MonitoringProposeMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO, MonitoringSysInfoMessage.class);
    }

    @Override
    public void onIncomingMessage(Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.MONITORING_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST:
                        if (!m_peerIsSuperpeer) {
                            incomingMonitoringDataRequest((MonitoringDataRequest) p_message);
                        }
                        break;
                    case MonitoringMessages.SUBTYPE_MONITORING_DATA:
                        LOGGER.trace("Received Monitoring-Data");

                        if (m_peerIsSuperpeer) {
                            incomingMonitoringData((MonitoringDataMessage) p_message);
                        }
                        break;
                    case MonitoringMessages.SUBTYPE_MONITORING_PROPOSE:
                        if (m_peerIsSuperpeer) {
                            incomingMonitoringPropose((MonitoringProposeMessage) p_message);
                        }
                        break;
                    case MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO:
                        if (m_peerIsSuperpeer) {
                            incomingMonitoringSystemInfo((MonitoringSysInfoMessage) p_message);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Handles received MonitoringSysInfoMessage by adding to a list.
     *
     * @param p_message MonitoringSysInfoMessage
     */
    private void incomingMonitoringSystemInfo(final MonitoringSysInfoMessage p_message) {
        m_monitor.addMonitoringSysInfoToWriter(p_message.getSource(), p_message.getWrapper());
    }

    /**
     * Adds incoming MonitoringData to List.
     *
     * @param p_message Message with Data
     */
    private void incomingMonitoringData(final MonitoringDataMessage p_message) {
        m_monitor.addMonitoringDataToWriter(p_message.getMonitorData()); // todo use chunkservice and put to superpeer
    }

    /**
     * Incoming propse message (currently beeing ignored)
     *
     * @param p_message
     */
    private void incomingMonitoringPropose(final MonitoringProposeMessage p_message) {
        LOGGER.debug("Received Monitoring propose from: %d - component %s value: %f",
                p_message.getSource(), p_message.getComponent(), p_message.getValue());
    }

    /**
     * Handle terminal monitoring data request.
     *
     * @param p_request Reqeust
     */
    private void incomingMonitoringDataRequest(final MonitoringDataRequest p_request) {
        Runnable task = () -> {
            MonitoringDataStructure monitorData = m_monitor.getCurrentMonitoringData();
            MonitoringDataResponse response = new MonitoringDataResponse(p_request, monitorData);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                LOGGER.error("Sending MonitorResponse for %s failed: %s", p_request, e);
            }
        };
        new Thread(task).start();
    }

    /**
     * Terminal method to get current monitoring data for a chosen peer.
     *
     * @param p_nid nid of peer
     * @return Monitornig Data
     */
    public MonitoringDataStructure getMonitoringDataFromPeer(final short p_nid) {
        if (m_boot.getNodeId() == p_nid) { // will never be the case because only terminal will call this method
            return m_monitor.getCurrentMonitoringData();
        }

        if (!m_boot.isNodeOnline(p_nid)) {
            return null;
        }

        if (!m_boot.getOnlineSuperpeerIds().contains(p_nid)) {
            MonitoringDataRequest request = new MonitoringDataRequest(p_nid);

            try {
                m_network.sendSync(request);
            } catch (NetworkException e) {
                e.printStackTrace();
            }

            MonitoringDataResponse response = (MonitoringDataResponse) request.getResponse();
            return response.getData();
        }

        return null;
    }
}
