package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.monitoring.messages.*;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Monitoring Service
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class MonitoringService extends AbstractDXRAMService<MonitoringServiceConfig> implements MessageReceiver {

    private boolean m_peerIsSuperpeer;

    private AbstractBootComponent m_boot;
    private MonitoringComponent m_monitor;
    private NetworkComponent m_network;

    //private ArrayList<String> m_supportedComponents = new ArrayList<>();

    /**
     * Constructor
     */
    public MonitoringService() {
        super("monitoring", MonitoringServiceConfig.class);
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_monitor = p_componentAccessor.getComponent(MonitoringComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean startService(DXRAMContext.Config p_config) {
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

    private void registerMessageReceiver() {
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA, this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE, this);
        m_network.register(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO, this);
    }

    private void unregisterMessageReceiver() {
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA, this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE, this);
        m_network.unregister(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO, this);
    }

    private void registerMonitorMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST, MonitoringDataRequest.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA_RESPONSE, MonitoringDataResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA, MonitoringDataMessage.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE, MonitoringProposeMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO, MonitoringSysInfoMessage.class);
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
                        // #if LOGGER == TRACE
                        LOGGER.trace("Received Monitoring-Data");
                        // #endif /* LOGGER == TRACE */
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

    private void incomingMonitoringSystemInfo(MonitoringSysInfoMessage p_message) {
        m_monitor.addMonitoringSysInfoToWriter(p_message.getSource(), p_message.getMonitoringSysInfoDataStructure());
    }

    private void incomingMonitoringData(final MonitoringDataMessage p_message) {
        m_monitor.addMonitoringDataToWriter(p_message.getMonitorData()); // todo use chunkservice and put to superpeer
    }

    private void incomingMonitoringPropose(final MonitoringProposeMessage p_message) {
        LOGGER.debug("Received Monitoring propose from: " + p_message.getSource() +
                " - component: " + p_message.getComponent() + " value: " + p_message.getValue());
    }

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

    /********************* TERMINAL FUNCTIONS *********************/
    // terminal uses this method to get monitoring info from peer
    public MonitoringDataStructure getMonitoringDataFromPeer(final short p_nid) {
        if (m_boot.getNodeID() == p_nid) { // will never be the case because only terminal will call this method
            return m_monitor.getCurrentMonitoringData();
        }

        if (!m_boot.isNodeOnline(p_nid)) {
            return null;
        }

        if (!m_boot.getIDsOfOnlineSuperpeers().contains(p_nid)) {
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
