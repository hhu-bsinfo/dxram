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

package de.hhu.bsinfo.utils.unit;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Wrapper for handling and converting storage units (byte, kb, mb, gb, tb)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class StorageUnit implements Importable, Exportable {

    public static final String BYTE = "b";
    public static final String KB = "kb";
    public static final String MB = "mb";
    public static final String GB = "gb";
    public static final String TB = "tb";

    private long m_bytes;

    /**
     * Constructor
     */
    public StorageUnit() {

    }

    /**
     * Constructor
     *
     * @param p_value
     *         Value
     * @param p_unit
     *         Unit of the value (b, kb, mb, gb, tb)
     */
    public StorageUnit(final long p_value, final String p_unit) {
        parse(p_value, p_unit.toLowerCase());
    }

    /**
     * Get as bytes
     *
     * @return Bytes
     */
    public long getBytes() {
        return m_bytes;
    }

    /**
     * Get as KB
     *
     * @return KB
     */
    public long getKB() {
        return m_bytes / 1024;
    }

    /**
     * Get as MB
     *
     * @return MB
     */
    public long getMB() {
        return m_bytes / 1024 / 1024;
    }

    /**
     * Get as GB
     *
     * @return GB
     */
    public long getGB() {
        return m_bytes / 1024 / 1024 / 1024;
    }

    /**
     * Get as TB
     *
     * @return TB
     */
    public long getTB() {
        return m_bytes / 1024 / 1024 / 1024 / 1024;
    }

    @Override
    public String toString() {
        return m_bytes + " bytes";
    }

    /**
     * Parse the value with the specified unit
     *
     * @param p_value
     *         Value
     * @param p_unit
     *         Unit of the value
     */
    private void parse(final long p_value, final String p_unit) {
        switch (p_unit) {
            case KB:
                m_bytes = p_value * 1024;
                break;
            case MB:
                m_bytes = p_value * 1024 * 1024;
                break;
            case GB:
                m_bytes = p_value * 1024 * 1024 * 1024;
                break;
            case TB:
                m_bytes = p_value * 1024 * 1024 * 1024 * 1024;
                break;
            case BYTE:
            default:
                m_bytes = p_value;
                break;
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_bytes);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_bytes = p_importer.readLong();
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }
}
