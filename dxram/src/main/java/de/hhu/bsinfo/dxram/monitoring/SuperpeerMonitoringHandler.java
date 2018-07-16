package de.hhu.bsinfo.dxram.monitoring;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Superpeer monitoring handler thread. Will write a list of nodes on event trigger.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class SuperpeerMonitoringHandler extends Thread implements EventListener<AbstractEvent> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(
            SuperpeerMonitoringHandler.class.getSimpleName());

    private ArrayList<MonitoringDataStructure> m_collectedData;
    private volatile boolean m_shouldShutdown;
    private HashMap<Short, MonitoringSysInfoDataStructure> m_sysInfos;

    private AbstractBootComponent m_bootComponent;
    private EventComponent m_eventComponent;

    private String m_monitoringFolder;
    private long m_secondDelay;
    private static final char DEFAULT_SEPARATOR = ',';
    private static final String LINE_SEPERATOR = "\n";

    /**
     * Constructor
     */
    SuperpeerMonitoringHandler(float p_secondDelay, AbstractBootComponent p_bootComponent,
            EventComponent p_eventComponent, String p_monitoringFolder) {
        m_collectedData = new ArrayList<>();
        m_sysInfos = new HashMap<>();
        m_shouldShutdown = false;
        m_secondDelay = (long) p_secondDelay;
        m_bootComponent = p_bootComponent;
        m_monitoringFolder = p_monitoringFolder;
        m_eventComponent = p_eventComponent;
    }

    void addDataToList(MonitoringDataStructure p_data) {
        m_collectedData.add(p_data);
    }

    void addSysInfoToList(short p_nid, MonitoringSysInfoDataStructure p_dataStructure) {
        m_sysInfos.put(p_nid, p_dataStructure);
        nodeOverview();
    }

    @Override
    public void run() {
        m_eventComponent.registerListener(this, NodeJoinEvent.class);
        m_eventComponent.registerListener(this, NodeFailureEvent.class);

        m_sysInfos.put(m_bootComponent.getNodeID(), new MonitoringSysInfoDataStructure());

        while (!m_shouldShutdown) {
            try {
                sleep(m_secondDelay * 1000);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException for %s", e);
            }
        }
    }

    /**
     * method needs to run more than once because not every node is online on first run (and nodes can boot later)
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

            for (Map.Entry<Short, MonitoringSysInfoDataStructure> set : m_sysInfos.entrySet()) {
                short nid = set.getKey();
                MonitoringSysInfoDataStructure data = set.getValue();

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
            nodesPW.close();
        }
    }

    void setShouldShutdown() {
        m_shouldShutdown = true;
    }

    @Override
    public void eventTriggered(AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent || p_event instanceof NodeJoinEvent) {
            nodeOverview();
        }
    }
}
