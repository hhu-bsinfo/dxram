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

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.stats.AbstractOperation;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Handler for DXRAM specific Monitoring Data (Uses currently only ChunkService, MemoryManagerComponent and DXNet Statistics)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class PeerDXRAMMonitoringHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(
            PeerDXRAMMonitoringHandler.class);

    private short m_ownNid;
    private volatile boolean m_shouldShutdown;
    private String m_monitoringFolder;

    private float m_secondDelay;
    private short m_numCollects;

    private StatisticsManager m_statsManager;
    private HashMap<String, PrintWriter> m_statsWriter;

    /**
     * Constructor
     *
     * @param p_ownNid           own node id
     * @param p_numberOfCollects number of collects
     * @param p_secondDelay      delay in seconds
     * @param p_monFolder        path to monitoring folder
     */
    PeerDXRAMMonitoringHandler(final short p_ownNid, final short p_numberOfCollects, final float p_secondDelay,
                               final String p_monFolder) {
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
            collectStatsFromClass(ChunkComponent.class);

            try {
                sleep((long) (m_secondDelay * 1000) / m_numCollects);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException for %s", e);
            }
        }

        for (PrintWriter writer : m_statsWriter.values()) {
            writer.close();
        }
    }

    /**
     * Collects statistics for a given class
     *
     * @param p_class
     */
    private void collectStatsFromClass(final Class<?> p_class) {
        ArrayList<AbstractOperation> operations = m_statsManager.getClassStatistics(p_class);

        for (AbstractOperation operation : operations) {
            PrintWriter writer = m_statsWriter.get(operation.getOperationName());

            if (writer == null) {
                writer = createPrintWriter(operation);

                if (writer == null) {
                    continue;
                }

                m_statsWriter.put(operation.getOperationName(), writer);
            }

            String csv = operation.toCSV(
                    ','); // TODO find better way to put in csv Format (for example ThroughputPool has bad keywords) - for example check with instanceof and put in datastructure and send to superpeer

            if (!csv.isEmpty()) {
                writer.println(csv);
                writer.flush();
            }
        }
    }

    /**
     * Creates a csv file and printwriter for a given operation
     *
     * @param operation
     * @return PrintWriter instance
     */
    private PrintWriter createPrintWriter(final AbstractOperation operation) {
        try {
            String path = m_monitoringFolder + File.separator + "node" + NodeID.toHexString(m_ownNid);
            File tmp = new File(path);

            if (!tmp.exists()) {
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
            writer.flush();

            return writer;
        } catch (Exception e) {
            LOGGER.error("Couldn't create PrintWriter " + e);
        }

        return null;
    }

    /**
     * Sets the shouldshutdown variable.
     */
    void setShouldShutdown() {
        m_shouldShutdown = true;
    }
}
