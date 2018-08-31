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

package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Stores the primary peer and the lookup range boundaries.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.09.2013
 */
public final class LookupRange implements Importable, Exportable {

    // Attributes
    private short m_primaryPeer;
    private long[] m_range;
    private LookupState m_state;

    /**
     * Default constructor
     */
    public LookupRange() {
        m_primaryPeer = NodeID.INVALID_ID;
        m_range = null;
        m_state = LookupState.OK;
    }

    // Constructors

    /**
     * Creates an instance of LookupRange
     *
     * @param p_primaryPeer
     *         the primary peer
     * @param p_range
     *         the range's beginning and ending
     * @param p_lookupState
     *         the state
     */
    public LookupRange(final short p_primaryPeer, final long[] p_range, final LookupState p_lookupState) {
        super();

        m_primaryPeer = p_primaryPeer;
        m_range = p_range;
        m_state = p_lookupState;
    }

    /**
     * Creates an instance of LookupRange
     *
     * @param p_lookupState
     *         the state
     */
    public LookupRange(final LookupState p_lookupState) {
        super();

        m_state = p_lookupState;
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
     *         the primary peer
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

    /**
     * Set the state
     *
     * @param p_state
     *         the status
     */
    public void setState(final LookupState p_state) {
        m_state = p_state;
    }

    /**
     * Get the state of lookup operation
     *
     * @return OK, DOES_NOT_EXIST, DATA_TEMPORARY_UNAVAILABLE or DATA_LOST
     */
    public LookupState getState() {
        return m_state;
    }

    // Getter

    @Override
    public void importObject(final Importer p_importer) {
        m_primaryPeer = p_importer.readShort(m_primaryPeer);
        if (m_range == null) {
            m_range = new long[2];
        }
        m_range[0] = p_importer.readLong(m_range[0]);
        m_range[1] = p_importer.readLong(m_range[1]);
        switch (p_importer.readByte((byte) 0) /* last byte cannot be skipped */) {
            case 0:
                m_state = LookupState.OK;
                break;
            case 1:
                m_state = LookupState.DOES_NOT_EXIST;
                break;
            case 2:
                m_state = LookupState.DATA_TEMPORARY_UNAVAILABLE;
                break;
            case 3:
                m_state = LookupState.DATA_LOST;
                break;
            default:
                break;
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_primaryPeer);
        p_exporter.writeLong(getStartID());
        p_exporter.writeLong(getEndID());
        switch (m_state) {
            case OK:
                p_exporter.writeByte((byte) 0);
                break;
            case DOES_NOT_EXIST:
                p_exporter.writeByte((byte) 1);
                break;
            case DATA_TEMPORARY_UNAVAILABLE:
                p_exporter.writeByte((byte) 2);
                break;
            case DATA_LOST:
                p_exporter.writeByte((byte) 3);
                break;
            default:
                break;
        }
    }

    @Override
    public int sizeofObject() {
        return Short.BYTES + 2 * Long.BYTES + Byte.BYTES;
    }

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

    // Setter

    /**
     * Get the start LocalID
     *
     * @return the start LocalID
     */
    private long getStartID() {
        if (m_range != null) {
            return m_range[0];
        } else {
            return ChunkID.INVALID_ID;
        }
    }

    // Methods

    /**
     * Get the end LocalID
     *
     * @return the end LocalID
     */
    private long getEndID() {
        if (m_range != null) {
            return m_range[1];
        } else {
            return ChunkID.INVALID_ID;
        }
    }
}
