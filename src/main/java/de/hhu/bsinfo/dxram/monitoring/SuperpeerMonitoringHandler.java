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

import de.hhu.bsinfo.dxmonitor.state.SystemState;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.monitoring.util.MonitoringSysDxramWrapper;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Superpeer monitoring handler thread. Will write a list of nodes on event trigger.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class SuperpeerMonitoringHandler extends Thread implements EventListener<AbstractEvent> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(
            SuperpeerMonitoringHandler.class);

    private ArrayList<MonitoringDataStructure> m_collectedData;
    private volatile boolean m_shouldShutdown;
    private HashMap<Short, MonitoringSysDxramWrapper> m_sysInfos;

    private AbstractBootComponent m_bootComponent;
    private EventComponent m_eventComponent;

    private String m_monitoringFolder;
    private long m_secondDelay;
    private static final char DEFAULT_SEPARATOR = ',';
    private static final String LINE_SEPERATOR = "\n";

    /**
     * Constructor
     *
     * @param p_secondDelay      delay in seconds
     * @param p_bootComponent    BootComponent Instance
     * @param p_eventComponent   EventComponent Instance
     * @param p_monitoringFolder path to monitoring folder
     */
    SuperpeerMonitoringHandler(final float p_secondDelay, final AbstractBootComponent p_bootComponent,
                               final EventComponent p_eventComponent, final String p_monitoringFolder) {
        m_collectedData = new ArrayList<>();
        m_sysInfos = new HashMap<>();
        m_shouldShutdown = false;
        m_secondDelay = (long) p_secondDelay;
        m_bootComponent = p_bootComponent;
        m_monitoringFolder = p_monitoringFolder;
        m_eventComponent = p_eventComponent;
    }

    /**
     * Adds monitoring data structured to list
     *
     * @param p_data Data Structure
     */
    void addDataToList(final MonitoringDataStructure p_data) {
        m_collectedData.add(p_data);
    }

    /**
     * Adds system information about a certain node to the hashmap
     *
     * @param p_nid     NID
     * @param p_wrapper Sysinfo Wrapper
     */
    void addSysInfoToList(final short p_nid, final MonitoringSysDxramWrapper p_wrapper) {
        m_sysInfos.put(p_nid, p_wrapper);
        nodeOverview();
    }

    @Override
    public void run() {
        m_eventComponent.registerListener(this, NodeJoinEvent.class);
        m_eventComponent.registerListener(this, NodeFailureEvent.class);


        String[] sysInfos = new String[5];
        String[] dxramInfos = new String[5];

        sysInfos[0] = SystemState.getKernelVersion();
        sysInfos[1] = SystemState.getDistribution();
        sysInfos[2] = SystemState.getCurrentWorkingDirectory();
        sysInfos[3] = SystemState.getHostName();
        sysInfos[4] = SystemState.getUserName();

        dxramInfos[0] = MonitoringDXRAMInformation.getBuildUser();
        dxramInfos[1] = MonitoringDXRAMInformation.getBuildDate();
        dxramInfos[2] = MonitoringDXRAMInformation.getCommit();
        dxramInfos[3] = MonitoringDXRAMInformation.getVersion();
        dxramInfos[4] = MonitoringDXRAMInformation.getBuildType();
        boolean isPageCacheInUse = MonitoringDXRAMInformation.isPageCacheInUse();

        m_sysInfos.put(m_bootComponent.getNodeId(),
                new MonitoringSysDxramWrapper(sysInfos, dxramInfos, isPageCacheInUse));

        while (!m_shouldShutdown) {
            try {
                sleep(m_secondDelay * 1000);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException for %s", e);
            }
        }
    }

    /**
     * Writes all sysinfos of current nodes to a csv file
     * This method needs to run more than once because not every node is online on first run (and nodes can boot later).
     */
    private void nodeOverview() {
        // create file with list of nodes
        PrintWriter nodesPW = null;

        try {
            File file = new File(m_monitoringFolder + File.separator + "nodes.csv");

            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF8");
            nodesPW = new PrintWriter(osw);
            nodesPW.println("nid,role,ip,port,kernel_version,distribution,cwd,host_name,logged_in_user," +
                    "build_user,build_date,dxram_commit,dxram_version,pagecache_enabled");

            for (Map.Entry<Short, MonitoringSysDxramWrapper> set : m_sysInfos.entrySet()) {
                short nid = set.getKey();
                MonitoringSysDxramWrapper data = set.getValue();

                NodeRole nodeRole = m_bootComponent.getNodeRole(nid);
                nodesPW.print(NodeID.toHexString(nid));
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(nodeRole);
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(m_bootComponent.getNodeAddress(nid).getHostString());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(m_bootComponent.getNodeAddress(nid).getPort());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getKernelVersion());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getDistribution());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getCWD());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getHostName());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getLoggedInUser());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getBuildUser());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getBuildDate());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getDxramCommit());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.getDxramVersion());
                nodesPW.print(DEFAULT_SEPARATOR);
                nodesPW.print(data.isPageCacheInUse());
                nodesPW.print(LINE_SEPERATOR);
            }

            nodesPW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (nodesPW != null) {
                nodesPW.close();
            }
        }
    }

    /**
     * Sets m_shouldShutdown to false.
     */
    void setShouldShutdown() {
        m_shouldShutdown = true;
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent) {
            m_sysInfos.remove(((NodeFailureEvent) p_event).getNodeID());
        }
    }
}
