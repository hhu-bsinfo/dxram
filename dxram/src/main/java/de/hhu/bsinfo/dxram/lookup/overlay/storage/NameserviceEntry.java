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

/**
 * Nameservice entry
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class NameserviceEntry {
    private int m_id;
    private long m_value;

    /**
     * Constructor
     *
     * @param p_id
     *     Id of the entry
     * @param p_value
     *     Value of the entry
     */
    public NameserviceEntry(final int p_id, final long p_value) {
        m_id = p_id;
        m_value = p_value;
    }

    /**
     * Get the id of the entry
     *
     * @return Id of the entry
     */
    public int getId() {
        return m_id;
    }

    /**
     * Get the value of the entry
     *
     * @return Value
     */
    public long getValue() {
        return m_value;
    }
}
