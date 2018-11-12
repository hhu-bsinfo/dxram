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

package de.hhu.bsinfo.dxram.util;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

/**
 * Represents the node roles.
 *
 * @author Florian Klein, florian.klein@hhu.de, 28.11.2013
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.12.2015
 */
public enum NodeRole {

    // Constants
    PEER('P'), SUPERPEER('S');

    public static final String SUPERPEER_STR = "superpeer";
    public static final String PEER_STR = "peer";

    // Attributes
    private final char m_acronym;

    // Constructors

    /**
     * Creates an instance of Role
     *
     * @param p_acronym
     *         the role's acronym
     */
    NodeRole(final char p_acronym) {
        m_acronym = p_acronym;
    }

    /**
     * Gets the acronym of the role
     *
     * @return the acronym
     */
    public char getAcronym() {
        return m_acronym;
    }

    // Getters

    /**
     * Get the node role from a full string.
     *
     * @param p_str
     *         String to parse.
     * @return Role node of string.
     */
    @JsonCreator
    public static NodeRole toNodeRole(final String p_str) {
        String str = p_str.toLowerCase();

        if (str.equals(SUPERPEER_STR) || "s".equals(str)) {
            return NodeRole.SUPERPEER;
        } else {
            return NodeRole.PEER;
        }
    }

    /**
     * Gets the role for the given acronym
     *
     * @param p_acronym
     *         the acronym
     * @return the corresponding role
     */
    public static NodeRole getRoleByAcronym(final char p_acronym) {
        NodeRole ret = null;

        for (NodeRole role : values()) {
            if (role.m_acronym == p_acronym) {
                ret = role;

                break;
            }
        }

        return ret;
    }

    /**
     * Assert node roles. This call is stripped on non-debug builds by the build system
     *
     * Throws an InvalidNodeRoleException on failure
     *
     * @param p_expected Node role expected
     * @param p_actual Actual node role
     */
    public static void assertNodeRole(final NodeRole p_expected, final NodeRole p_actual) {
        if (p_expected != p_actual) {
            throw new InvalidNodeRoleException(p_actual);
        }
    }

    // Methods

    @Override
    @JsonValue
    public String toString() {
        if (equals(PEER)) {
            return PEER_STR;
        } else {
            return SUPERPEER_STR;
        }
    }
}
