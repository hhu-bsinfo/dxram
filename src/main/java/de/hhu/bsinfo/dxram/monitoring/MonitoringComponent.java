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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.management.MBeanServer;

import de.hhu.bsinfo.dxmonitor.util.DeviceLister;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogComponentConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.monitoring.util.MonitoringSysDxramWrapper;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Monitoring component (will launch 2 handler threads on peer nodes and 1 handler on superpeers)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.MONITORING,
        priorityShutdown = DXRAMComponentOrder.Shutdown.MONITORING)
public class MonitoringComponent extends AbstractDXRAMComponent<MonitoringComponentConfig> {
    private PeerMonitoringHandler m_peerHandler;
    private PeerDXRAMMonitoringHandler m_dxramPeerHandler;
    private SuperpeerMonitoringHandler m_superpeerHandler;

    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private EventComponent m_event;
    private ChunkComponent m_chunk;

    private static final String VIRTUALMACHINE_CLASS = "com.sun.tools.attach.VirtualMachine";

    private final MBeanServer m_beanServer = ManagementFactory.getPlatformMBeanServer();

    /**
     * Returns true if monitoring is activated.
     */
    public boolean isActive() {
        return getConfig().isMonitoringActive();
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        if (!getConfig().isMonitoringActive()) {
            return true;
        }

        if (!isToolsJarLoaded()) {
            LOGGER.error("The monitoring system requires tools.jar to be on the classpath");
            return false;
        }

        LogComponentConfig logConfig = p_config.getComponentConfig(LogComponent.class);
        MonitoringComponentConfig componentConfig = p_config.getComponentConfig(MonitoringComponent.class);

        // Create directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(componentConfig.getMonitoringFolder()));
        } catch (IOException e) {
            LOGGER.error("Failed creating monitoring data directory");
            return false;
        }

        if (componentConfig.isMonitoringActive()) {
            String diskIdentifier = componentConfig.getDisk();

            if (diskIdentifier.isEmpty()) {
                // pick first disk found
                try {
                    diskIdentifier = DeviceLister.getDisks().get(0);
                } catch (final IOException e) {
                    LOGGER.error("Failed to auto assign first disk identifier: %s", e.getMessage());
                    return false;
                }

                LOGGER.warn("Empty disk identifier from config, auto assigning disk: %s", diskIdentifier);
            }

            String nicIdentifier = componentConfig.getNic();

            if (nicIdentifier.isEmpty()) {
                try {
                    nicIdentifier = DeviceLister.getNICs().get(0);
                } catch (final IOException e) {
                    LOGGER.error("Failed to auto assign first NIC identifier: %s", e.getMessage());
                    return false;
                }

                LOGGER.warn("Empty NIC identifier from config, auto assigning interface: %s", nicIdentifier);
            }

            String monitoringFolder = componentConfig.getMonitoringFolder();
            float secondDelay = componentConfig.getTimeWindow().getSec();

            // check if kernel buffer is in use
            boolean isPageCacheInUse = false;
            String hardwareAccessMode =logConfig.getDxlogConfig().getHarddriveAccess();

            if (hardwareAccessMode.equals("raf")) {
                isPageCacheInUse = true;
            }

            String buildUser = BuildConfig.BUILD_USER;
            String buildDate = BuildConfig.BUILD_DATE;
            String buildType = BuildConfig.BUILD_TYPE;
            String commit = BuildConfig.GIT_COMMIT;
            String version = BuildConfig.DXRAM_VERSION.toString();

            MonitoringDXRAMInformation.setValues(buildDate, buildUser, buildType, version, commit, isPageCacheInUse);

            short numberOfCollects = componentConfig.getCollectsPerWindow();

            if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
                m_superpeerHandler =
                        new SuperpeerMonitoringHandler(componentConfig.getCsvTimeWindow().getSec(), m_boot, m_event,
                                monitoringFolder);
                m_superpeerHandler.start();
            } else {
                short ownNid = m_boot.getNodeId();
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

                m_dxramPeerHandler =
                        new PeerDXRAMMonitoringHandler(ownNid, numberOfCollects, secondDelay, monitoringFolder);
                m_dxramPeerHandler.start();
            }
        }

        return true;
    }

    /**
     * Checks if tools.jar is included within the classpath.
     *
     * @return True, if tools.jar is included; false else
     */
    private boolean isToolsJarLoaded() {
        try {
            Class.forName(VIRTUALMACHINE_CLASS, false, this.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            return false;
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

    /**
     * Wrapper method (only needed by terminal currently) to get current monitoring data.
     *
     * @return Monitoring data
     */
    MonitoringDataStructure getCurrentMonitoringData() {
        return m_peerHandler.getMonitoringData();
    }

    /**
     * Adds monitoring data to superpeer handlers list.
     */
    void addMonitoringDataToWriter(final MonitoringDataStructure p_data) {
        m_superpeerHandler.addDataToList(p_data);
    }

    /**
     * Adds system information to superpeer handler
     *
     * @param p_nid
     *         NID of node who send the system information
     * @param p_wrapper
     *         Wrapper class instance which stores the monitoring information
     */
    void addMonitoringSysInfoToWriter(final short p_nid, final MonitoringSysDxramWrapper p_wrapper) {
        m_superpeerHandler.addSysInfoToList(p_nid, p_wrapper);
    }
}
