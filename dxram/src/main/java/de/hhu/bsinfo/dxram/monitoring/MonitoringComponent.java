package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxmonitor.util.DeviceLister;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.log.LogComponentConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.monitoring.util.MonitoringSysDxramWrapper;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.ManifestHelper;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Monitoring component (will launch 2 handler threads on peer nodes and 1 handler on superpeers)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class MonitoringComponent extends AbstractDXRAMComponent<MonitoringComponentConfig> {

    private PeerMonitoringHandler m_peerHandler;
    private PeerDXRAMMonitoringHandler m_dxramPeerHandler;
    private SuperpeerMonitoringHandler m_superpeerHandler;

    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private EventComponent m_event;

    public MonitoringComponent() {
        super(DXRAMComponentOrder.Init.MONITORING, DXRAMComponentOrder.Shutdown.MONITORING,
                MonitoringComponentConfig.class);
    }

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(DXRAMContext.Config p_config) {
        MonitoringComponentConfig componentConfig = p_config.getComponentConfig(MonitoringComponentConfig.class);

        String diskIdentifier = componentConfig.getDisk();

        if (diskIdentifier.isEmpty()) {
            // pick first disk found
            diskIdentifier = DeviceLister.getDisks().get(0);

            LOGGER.warn("Empty disk identifier from config, auto assigning disk: %s", diskIdentifier);
        }

        String nicIdentifier = componentConfig.getNic();

        if (nicIdentifier.isEmpty()) {
            nicIdentifier = DeviceLister.getNICs().get(0);

            LOGGER.warn("Empty NIC identifier from config, auto assigning interface: %s", nicIdentifier);
        }

        String monitoringFolder = componentConfig.getMonitoringFolder();
        float secondDelay = componentConfig.getSecondsTimeWindow();

        // check if kernel buffer is in use
        boolean isPageCacheInUse = false;
        String hardwareAccessMode = p_config.getComponentConfig(LogComponentConfig.class).getHarddriveAccess();

        if (hardwareAccessMode.equals("raf")) {
            isPageCacheInUse = true;
        }

        String buildUser = ManifestHelper.getProperty(getClass(), "BuildUser");
        String buildDate = ManifestHelper.getProperty(getClass(), "BuildDate");
        String buildType = DXRAM.BUILD_TYPE;
        String commit = DXRAM.GIT_COMMIT;
        String version = DXRAM.VERSION.toString();

        MonitoringDXRAMInformation.setValues(buildDate, buildUser, buildType, version, commit, isPageCacheInUse);

        short numberOfCollects = componentConfig.getCollectsPerWindow();

        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            m_superpeerHandler = new SuperpeerMonitoringHandler(componentConfig.getCSVSecondsTimeWindow(), m_boot,
                    m_event, monitoringFolder);
            m_superpeerHandler.start();
        } else {
            short ownNid = m_boot.getNodeID();
            short superpeerNid = m_lookup.getResponsibleSuperpeer(ownNid);

            if (superpeerNid == NodeID.INVALID_ID) {
                LOGGER.error("Found no responsible superpeer for node 0x%x", ownNid);
                return false; // need superpeer to monitor
            }

            m_peerHandler = new PeerMonitoringHandler(ownNid, superpeerNid, m_network);
            m_peerHandler.setConfigParameters(monitoringFolder, secondDelay, numberOfCollects, nicIdentifier,
                    diskIdentifier);
            m_peerHandler.setupComponents();
            m_peerHandler.start();

            m_dxramPeerHandler = new PeerDXRAMMonitoringHandler(ownNid, numberOfCollects, secondDelay,
                    monitoringFolder);
            m_dxramPeerHandler.start();

        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_peerHandler != null) {
            m_peerHandler.setShouldShutdown();

            try {
                m_peerHandler.join();
            } catch (InterruptedException e) {
                LOGGER.error("Coulnd't join peer component handler thread", e);
            }
        }

        if (m_dxramPeerHandler != null) {
            m_dxramPeerHandler.setShouldShutdown();

            try {
                m_dxramPeerHandler.join();
            } catch (InterruptedException e) {
                LOGGER.error("Coulnd't join peer dxram monitoring handler thread", e);
            }
        }

        if (m_superpeerHandler != null) {
            m_superpeerHandler.setShouldShutdown();

            try {
                m_superpeerHandler.join();
            } catch (InterruptedException e) {
                LOGGER.error("Coulnd't join superpeer handler thread", e);
            }
        }
        return true;
    }

    /****** TERMINAL FUNCTIONS ******/
    MonitoringDataStructure getCurrentMonitoringData() {
        return m_peerHandler.getMonitoringData();
    }

    /****** WRAPPER FOR HANDLER ******/
    void addMonitoringDataToWriter(MonitoringDataStructure p_data) {
        m_superpeerHandler.addDataToList(p_data);
    }

    void addMonitoringSysInfoToWriter(short p_nid, MonitoringSysDxramWrapper p_wrapper) {
        m_superpeerHandler.addSysInfoToList(p_nid, p_wrapper);
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }
}
