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

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Result object for sign on to a barrier
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.2017
 */
public class BarrierStatus implements Importable, Exportable {
    private short m_numSignedOnPeers;
    private short[] m_signedOnNodeIDs = new short[0];
    private long[] m_customData = new long[0];

    /**
     * Default constructor
     */
    public BarrierStatus() {

    }

    /**
     * Constructor
     *
     * @param p_numSignedOnPeers
     *         Number of peers that signed on so far
     * @param p_signedOnNodeIDs
     *         Array of Node IDs that signed on to the barrier
     * @param p_customData
     *         Array of custom data received from the signed on nodes
     */
    public BarrierStatus(final short p_numSignedOnPeers, final short[] p_signedOnNodeIDs, final long[] p_customData) {
        m_numSignedOnPeers = p_numSignedOnPeers;
        m_signedOnNodeIDs = p_signedOnNodeIDs;
        m_customData = p_customData;
    }

    /**
     * Get the number of actually signed on peers. This can be less than
     * the array sizes of signed on node ids and custom data if the sign
     * on process is still ongoing
     *
     * @return Number of currently signed on peers of this status
     */
    public short getNumberOfSignedOnPeers() {
        return m_numSignedOnPeers;
    }

    /**
     * Get the array of signed on node IDs
     *
     * @return Array of node IDs
     */
    public short[] getSignedOnNodeIDs() {
        return m_signedOnNodeIDs;
    }

    /**
     * Get the array of custom data delivered by the signed on nodes
     *
     * @return Array of custom data (indices match node id array)
     */
    public long[] getCustomData() {
        return m_customData;
    }

    /**
     * Find custom data provided by a specific node
     *
     * @param p_nodeId
     *         Node id to find custom data for
     * @return If the node id signed on with custom data returns the data, null otherwise
     */
    public Long findCustomData(final short p_nodeId) {
        for (int i = 0; i < m_signedOnNodeIDs.length; i++) {
            if (m_signedOnNodeIDs[i] == p_nodeId) {
                return m_customData[i];
            }
        }

        return null;
    }

    /**
     * Iterate the list of signed on peers with their provided custom data
     *
     * @param p_consumer
     *         Block to execute for every signed on peer
     */
    public void forEachSignedOnPeer(final Consumer p_consumer) {
        for (int i = 0; i < m_numSignedOnPeers; i++) {
            p_consumer.forEach(m_signedOnNodeIDs[i], m_customData[i]);
        }
    }

    @Override
    public String toString() {
        return "m_numSignedOnPeers " + m_numSignedOnPeers + ", m_signedOnNodeIDs " + NodeID.nodeIDArrayToString(
                m_signedOnNodeIDs) + ", m_customData " + ChunkID.chunkIDArrayToString(m_customData);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_numSignedOnPeers);
        p_exporter.writeShortArray(m_signedOnNodeIDs);
        p_exporter.writeLongArray(m_customData);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numSignedOnPeers = p_importer.readShort(m_numSignedOnPeers);
        m_signedOnNodeIDs = p_importer.readShortArray(m_signedOnNodeIDs);
        m_customData = p_importer.readLongArray(m_customData);
    }

    @Override
    public int sizeofObject() {
        return Short.BYTES + ObjectSizeUtil.sizeofShortArray(m_signedOnNodeIDs) + ObjectSizeUtil.sizeofLongArray(
                m_customData);
    }

    @FunctionalInterface
    public interface Consumer {
        void forEach(final short p_signedOnPeer, final long p_customData);
    }
}
