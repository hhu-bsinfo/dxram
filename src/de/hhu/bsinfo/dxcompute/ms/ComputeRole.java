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

package de.hhu.bsinfo.dxcompute.ms;

/**
 * Different compute roles of the master slave framework.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public enum ComputeRole {
    MASTER('M'), SLAVE('S'), NONE('N');

    public static final String MASTER_STR = "master";
    public static final String SLAVE_STR = "slave";
    public static final String NONE_STR = "none";

    private char m_acronym;

    /**
     * Creates an instance of Role
     * @param p_acronym
     *            the role's acronym
     */
    ComputeRole(final char p_acronym) {
        m_acronym = p_acronym;
    }

    /**
     * Get the node role from a full string.
     * @param p_str
     *            String to parse.
     * @return Role node of string.
     */
    public static ComputeRole toComputeRole(final String p_str) {
        String str = p_str.toLowerCase();
        switch (str) {
            case MASTER_STR:
            case "m":
                return ComputeRole.MASTER;
            case SLAVE_STR:
            case "s":
                return ComputeRole.SLAVE;
            default:
                return ComputeRole.NONE;
        }
    }

    /**
     * Gets the acronym of the role
     * @return the acronym
     */
    public char getAcronym() {
        return m_acronym;
    }

    @Override
    public String toString() {
        if (equals(MASTER)) {
            return MASTER_STR;
        } else if (equals(SLAVE)) {
            return SLAVE_STR;
        } else {
            return NONE_STR;
        }
    }

    /**
     * Gets the role for the given acronym
     * @param p_acronym
     *            the acronym
     * @return the corresponding role
     */
    public static ComputeRole getRoleByAcronym(final char p_acronym) {
        ComputeRole ret = null;

        for (ComputeRole role : values()) {
            if (role.m_acronym == p_acronym) {
                ret = role;

                break;
            }
        }

        return ret;
    }
}
