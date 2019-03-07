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

import de.hhu.bsinfo.dxmonitor.monitor.*;
import de.hhu.bsinfo.dxmonitor.state.StateUpdateException;
import de.hhu.bsinfo.dxmonitor.state.SystemState;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.monitoring.messages.MonitoringDataMessage;
import de.hhu.bsinfo.dxram.monitoring.messages.MonitoringProposeMessage;
import de.hhu.bsinfo.dxram.monitoring.messages.MonitoringSysInfoMessage;
import de.hhu.bsinfo.dxram.monitoring.metric.AverageMetric;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * PeerMonitoringHandler class
 * Peer collects monitoring data and sends it to superpeer.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class PeerMonitoringHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(PeerMonitoringHandler.class);

    private NetworkComponent m_networkComponent;

    private String m_monitorFolder;
    private PrintWriter m_writer;

    private HashMap<String, Monitor> m_monitors;
    private ArrayList<MonitoringDataStructure> m_monitoringDatas;

    private volatile boolean m_shouldShutdown;
    private float m_secondDelay;
    private String m_nicIdentifier;
    private String m_diskIdentifier;

    private short m_ownNid;
    private short m_superpeerNid;
    private short m_numberOfCollects;

    /**
     * Constructor
     *
     * @param p_ownNid           own node id
     * @param p_superpeerNid     NID of corresponding superpeer
     * @param p_networkComponent NetworkComponent instance to send messages
     */
    PeerMonitoringHandler(final short p_ownNid, final short p_superpeerNid, NetworkComponent p_networkComponent) {
        setName("PeerMonitoringHandler");

        m_monitors = new HashMap<>();
        m_monitoringDatas = new ArrayList<>(); // alternative use MonitoringDataStructure[numberOfCollects]
        m_shouldShutdown = false;

        m_ownNid = p_ownNid;
        m_superpeerNid = p_superpeerNid;
        m_networkComponent = p_networkComponent;
    }

    /**
     * Sets a few config values.
     *
     * @param p_monFolder        path to monitoring folder
     * @param p_secondDelay      delay in seconds
     * @param p_numberOfCollects number of collects per "time window"
     * @param p_nicIdentifier    nic identifier
     * @param p_diskIdentifier   disk identifier
     */
    void setConfigParameters(final String p_monFolder, final float p_secondDelay, final short p_numberOfCollects,
                             final String p_nicIdentifier, final String p_diskIdentifier) {
        m_monitorFolder = p_monFolder;
        m_secondDelay = p_secondDelay;
        m_numberOfCollects = p_numberOfCollects;
        m_nicIdentifier = p_nicIdentifier;
        m_diskIdentifier = p_diskIdentifier;
    }

    /**
     * Initializes the monitoring classes and assigns callbacks.
     */
    void setupComponents() {
        CpuMonitor cpu = new CpuMonitor();
        cpu.addThresholdCpuUsagePercent(
                new MultipleThresholdDouble("CpuUsage1", 3.9, true, 3, this::callbackCpuUsageThresholdExceed));
        m_monitors.put("cpu", cpu);

        MemMonitor memMonitor = new MemMonitor();
        memMonitor.addThresholdMemoryFree(
                new MultipleThresholdDouble("MemFree1", 0.8, true, 2, this::callbackMemThresholdExceed));
        m_monitors.put("memory", new MemMonitor());

        m_monitors.put("network", new NetworkMonitor(m_nicIdentifier));
        m_monitors.put("disk", new DiskMonitor(m_diskIdentifier));
        m_monitors.put("jvmmem", new JVMMemMonitor());
        m_monitors.put("jvmthreads", new JVMThreadsMonitor());
    }

    @Override
    public void run() {
        createPrintWriter();

        try {
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

            MonitoringSysInfoMessage message = new MonitoringSysInfoMessage(m_superpeerNid,
                    sysInfos, dxramInfos, isPageCacheInUse);
            m_networkComponent.sendMessage(message);
        } catch (NetworkException e) {
            e.printStackTrace();
        }

        while (!m_shouldShutdown) {
            m_monitoringDatas.add(getMonitoringData());

            if (m_monitoringDatas.size() == m_numberOfCollects) {
                sendDataToSuperpeer();
                m_monitoringDatas.clear();
            }

            try {
                sleep((long) (m_secondDelay * 1000) / m_numberOfCollects);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException for %s", e);
            }
        }
    }

    /**
     * Applies on collected monitoring datas the average metric and sends a single datastructure to the superpeer.
     * Furthermore the csv file is appended.
     */
    private void sendDataToSuperpeer() {
        MonitoringDataStructure data = AverageMetric.calculate(m_monitoringDatas);
        MonitoringDataMessage dataMessage = new MonitoringDataMessage(m_superpeerNid, data);

        try {
            m_networkComponent.sendMessage(dataMessage);
        } catch (NetworkException e) {
            LOGGER.error("Sending MonitoringDataMessage for %f failed: %s", m_superpeerNid, e);
        }

        appendDataToFile(data);
    }

    /**
     * Returns certain monitor class.
     *
     * @param p_key key
     * @return Monitor class for given key
     */
    public Monitor getComponent(final String p_key) {
        return m_monitors.get(p_key);
    }

    /**
     * Collects monitoring data and puts them in a monitoring data structure
     *
     * @return the created DS
     */
    MonitoringDataStructure getMonitoringData() {
        MonitoringDataStructure monitoringData = new MonitoringDataStructure(m_ownNid, System.nanoTime());

        for (Monitor monitor : m_monitors.values()) {
            try {
                monitor.update();
            } catch (StateUpdateException e) {
                e.printStackTrace();
            }

            monitoringData.fillWithData(monitor);
        }

        return monitoringData;
    }

    /**
     * Sets the shutdown variable.
     */
    void setShouldShutdown() {
        m_shouldShutdown = true;
    }

    /**
     * Creates a printwriter class instance (and creates a csv file which stores general monitoring information)
     */
    private void createPrintWriter() {
        try {
            String path = m_monitorFolder + File.separator + "node" + NodeID.toHexString(m_ownNid);
            File tmp = new File(path);
            if (!tmp.exists()) {
                tmp.mkdir();
            }

            path += File.separator + "general.csv";
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF8");
            m_writer = new PrintWriter(osw);
            m_writer.println("nid,cpu,memory,rxThroughput,rxError,txThroughput,txError,readPercent,writePercent," +
                    "jvmHeapUsage,jvmEdenUsage,jvmSurvivorUsage,jvmOldUsage,jvmThreadDaemon,jvmThreadNonDaemon,jvmThreadCnt,jvmPeakCnt,timestamp");
            m_writer.flush();
        } catch (Exception e) {
            LOGGER.error("Couldn't create PrintWriter " + e);
        }
    }

    /**
     * Appends the general.csv file.
     *
     * @param p_data Monitoring Data
     */
    private void appendDataToFile(final MonitoringDataStructure p_data) {
        StringBuilder builder = new StringBuilder("");
        char DEFAULT_SEPARATOR = ',';
        builder.append(NodeID.toHexString(p_data.getNid()));
        builder.append(DEFAULT_SEPARATOR);

        builder.append(p_data.getCpuUsage());
        builder.append(DEFAULT_SEPARATOR);

        builder.append(p_data.getMemoryUsage());
        builder.append(DEFAULT_SEPARATOR);

        //network
        float[] tmp = p_data.getNetworkStats();
        builder.append(tmp[0]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[1]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[2]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[3]);
        builder.append(DEFAULT_SEPARATOR);

        // disk
        tmp = p_data.getDiskStats();
        builder.append(tmp[0]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[1]);
        builder.append(DEFAULT_SEPARATOR);

        //jvm
        tmp = p_data.getJvmMemStats();
        builder.append(tmp[0]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[1]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[2]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(tmp[3]);
        builder.append(DEFAULT_SEPARATOR);

        long[] threadTmp = p_data.getJvmThreadStats();
        builder.append(threadTmp[0]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(threadTmp[1]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(threadTmp[2]);
        builder.append(DEFAULT_SEPARATOR);
        builder.append(threadTmp[3]);
        builder.append(DEFAULT_SEPARATOR);

        builder.append(p_data.getTimestamp());

        m_writer.println(builder.toString());
        m_writer.flush();
    }

    /**
     * Sends a propose message to the superpeer because a threshold has been exceeded.
     *
     * @param p_component which component caused the propose
     * @param p_value     Actual value of component
     */
    private void sendProposeToSuperpeer(final String p_component, final double p_value) {
        MonitoringProposeMessage proposeMessage = new MonitoringProposeMessage(m_superpeerNid, p_component, p_value);

        try {
            m_networkComponent.sendMessage(proposeMessage);
        } catch (NetworkException e) {
            LOGGER.error("Sending MonitoringDataMessage for %f failed: %s", m_superpeerNid, e);
        }
    }

    /**
     * Callback Function for cpu usage.
     *
     * @param p_currentValue current cpu usage
     * @param p_threshold    threshold class instance
     */
    private void callbackCpuUsageThresholdExceed(final double p_currentValue,
                                                 final MultipleThresholdDouble p_threshold) {
        sendProposeToSuperpeer("cpu", p_currentValue);
    }

    /**
     * Callback function for memory.
     *
     * @param p_currentValue current memory "value"
     * @param p_threshold    threshold class instance
     */
    private void callbackMemThresholdExceed(final double p_currentValue, final MultipleThresholdDouble p_threshold) {
        sendProposeToSuperpeer("memory", p_currentValue);
    }
}
