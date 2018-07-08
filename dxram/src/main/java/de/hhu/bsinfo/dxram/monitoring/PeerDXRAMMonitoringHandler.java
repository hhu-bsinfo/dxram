package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.stats.AbstractOperation;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Handler for DXRAM specific Monitoring Data (Uses currently only ChunkService, MemoryManagerComponent and DXNet Statistics)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class PeerDXRAMMonitoringHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(PeerDXRAMMonitoringHandler.class.getSimpleName());

    private short m_ownNid;
    private volatile boolean m_shouldShutdown;
    private String m_monitoringFolder;

    private float m_secondDelay;
    private short m_numCollects;

    private StatisticsManager m_statsManager;
    private HashMap<String, PrintWriter> m_statsWriter;


    public PeerDXRAMMonitoringHandler(final short p_ownNid, short p_numberOfCollects, final float p_secondDelay, final String p_monFolder) {
        m_ownNid = p_ownNid;
        m_shouldShutdown = false;
        m_statsWriter = new HashMap<>();
        m_statsManager = StatisticsManager.get();

        m_monitoringFolder = p_monFolder;
        m_secondDelay = p_secondDelay;
        m_numCollects = p_numberOfCollects;
    }

    @Override
    public void run() {
        while (!m_shouldShutdown) {
            collectStatsFromClass(ChunkService.class);
            collectStatsFromClass(DXNet.class);
            collectStatsFromClass(MemoryManagerComponent.class);

            try {
                sleep((long) (m_secondDelay * 1000) / m_numCollects);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException for %s", e);
            }
        }

        for(PrintWriter writer : m_statsWriter.values()) {
            writer.close();
        }
    }

    private void collectStatsFromClass(final Class<?> p_class) {
        ArrayList<AbstractOperation> operations = m_statsManager.getClassStatistics(p_class);
        for (AbstractOperation operation : operations) {
            PrintWriter writer = m_statsWriter.get(operation.getOperationName());
            if(writer == null) {
                writer = createPrintWriter(operation);
                if(writer == null) continue;
                m_statsWriter.put(operation.getOperationName(), writer);
            }

            String csv = operation.toCSV(','); // TODO find better way to put in csv Format (for example ThroughputPool has bad keywords) - for example check with instanceof and put in datastructure and send to superpeer
            if(!csv.isEmpty()) {
                writer.println(csv);
                writer.flush();
            }
        }
    }

    private PrintWriter createPrintWriter(AbstractOperation operation) {
        try {
            String path = m_monitoringFolder + File.separator + "node" + NodeID.toHexString(m_ownNid);
            File tmp = new File(path);
            if(!tmp.exists()) {
                tmp.mkdir();
            }

            path += File.separator + operation.getOperationName() + ".csv";
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF8");
            PrintWriter writer = new PrintWriter(osw);
            writer.println(operation.generateCSVHeader(','));
            writer.flush();;
            return writer;
        } catch (Exception e) {
            LOGGER.error("Couldn't create PrintWriter " + e);
        }

        return null;
    }

    public void setShouldShutdown() {
        m_shouldShutdown = true;
    }
}
