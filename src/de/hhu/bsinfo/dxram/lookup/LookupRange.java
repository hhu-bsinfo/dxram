/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Stores the primary peer and the lookup range boundaries.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.09.2013
 */
public final class LookupRange implements Importable, Exportable {

    // Attributes
    private short m_primaryPeer;
    private long[] m_range;

    /**
     * Default constructor
     */
    public LookupRange() {
        m_primaryPeer = -1;
        m_range = null;
    }

    // Constructors

    /**
     * Creates an instance of LookupRange
     *
     * @param p_primaryPeer
     *     the primary peer
     * @param p_range
     *     the range's beginning and ending
     */
    public LookupRange(final short p_primaryPeer, final long[] p_range) {
        super();

        m_primaryPeer = p_primaryPeer;
        m_range = p_range;
    }

    /**
     * Get primary peer
     *
     * @return the primary peer
     */
    public short getPrimaryPeer() {
        return m_primaryPeer;
    }

    /**
     * Set primary peer
     *
     * @param p_primaryPeer
     *     the primary peer
     */
    public void setPrimaryPeer(final short p_primaryPeer) {
        m_primaryPeer = p_primaryPeer;
    }

    /**
     * Get range
     *
     * @return the beginning and ending of range
     */
    public long[] getRange() {
        return m_range;
    }

    // Getter

    /**
     * Get the start LocalID
     *
     * @return the start LocalID
     */
    private long getStartID() {
        return m_range[0];
    }

    /**
     * Get the end LocalID
     *
     * @return the end LocalID
     */
    private long getEndID() {
        return m_range[1];
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_primaryPeer = p_importer.readShort();
        m_range = new long[] {p_importer.readLong(), p_importer.readLong()};
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_primaryPeer);
        p_exporter.writeLong(getStartID());
        p_exporter.writeLong(getEndID());
    }

    // Setter

    @Override
    public int sizeofObject() {
        return Short.BYTES + 2 * Long.BYTES;
    }

    // Methods

    /**
     * Prints the LookupRange
     *
     * @return String interpretation of LookupRange
     */
    @Override
    public String toString() {
        String ret;

        ret = NodeID.toHexString(m_primaryPeer);
        if (m_range != null) {
            ret += ", (" + ChunkID.toHexString(m_range[0]) + ", " + ChunkID.toHexString(m_range[1]) + ')';
        }
        return ret;
    }
}
