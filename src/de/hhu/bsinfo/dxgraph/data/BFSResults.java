/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraph.data;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Data structure holding BFS results of multiple nodes.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 20.05.2016
 */
public class BFSResults extends DataStructure {
    private BFSResult m_aggregatedResult = new BFSResult();
    private Map<Integer, BFSResult> m_bfsResults = new HashMap<>();

    private int m_size; // For serialization, only

    /**
     * Constructor
     */
    public BFSResults() {

    }

    /**
     * Get aggregated results of all nodes.
     *
     * @return Aggregated results.
     */
    public BFSResult getAggregatedResult() {
        return m_aggregatedResult;
    }

    /**
     * Add a result of a node running BFS.
     *
     * @param p_computeSlaveId
     *         Compute slave id of the node.
     * @param p_nodeId
     *         Node id of the node.
     * @param p_bfsResult
     *         BFS result of the node.
     */
    public void addResult(final short p_computeSlaveId, final short p_nodeId, final BFSResult p_bfsResult) {
        m_bfsResults.put(p_nodeId << 16 | p_computeSlaveId, p_bfsResult);
    }

    /**
     * Get all single results of every BFS node.
     *
     * @return List of single results identified by compute slave id and node id.
     */
    public Map<Integer, BFSResult> getResults() {
        return m_bfsResults;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        m_aggregatedResult.exportObject(p_exporter);

        p_exporter.writeInt(m_bfsResults.size());
        for (Map.Entry<Integer, BFSResult> entry : m_bfsResults.entrySet()) {
            p_exporter.writeInt(entry.getKey());
            entry.getValue().exportObject(p_exporter);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_aggregatedResult.importObject(p_importer);

        m_size = p_importer.readInt(m_size);
        for (int i = 0; i < m_size; i++) {
            int id = p_importer.readInt(0);
            BFSResult result = new BFSResult();
            result.importObject(p_importer);
            if (m_bfsResults.size() == i) {
                m_bfsResults.put(id, result);
            }
        }
    }

    @Override
    public int sizeofObject() {
        int size = m_aggregatedResult.sizeofObject();
        size += Integer.BYTES;
        for (Map.Entry<Integer, BFSResult> entry : m_bfsResults.entrySet()) {
            size += Integer.BYTES + entry.getValue().sizeofObject();
        }
        return size;
    }

    @Override
    public String toString() {
        String str = "";

        str += "Aggregated result:\n" + m_aggregatedResult + "\n--------------------------";

        str += "Node count " + m_bfsResults.size();
        for (Map.Entry<Integer, BFSResult> entry : m_bfsResults.entrySet()) {
            str += "\n>>>>> " + NodeID.toHexString((short) (entry.getKey() >> 16)) + " | " + (entry.getKey() & 0xFFFF) + ": \n";
            str += entry.getValue();
        }

        return str;
    }
}
