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

package de.hhu.bsinfo.dxram.nameservice;

/**
 * Nameservice entry
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class NameserviceEntryStr {
    private String m_name;
    private long m_value;

    /**
     * Constructor
     *
     * @param p_name
     *     Name of the entry
     * @param p_value
     *     Value of the entry
     */
    public NameserviceEntryStr(final String p_name, final long p_value) {
        m_name = p_name;
        m_value = p_value;
    }

    /**
     * Get the name of the entry
     *
     * @return Name as string
     */
    public String getName() {
        return m_name;
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
