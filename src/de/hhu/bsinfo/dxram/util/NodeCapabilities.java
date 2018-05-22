/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.util;

import java.util.Arrays;

/**
 * Represents a node's capabilities.
 *
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class NodeCapabilities {

    public static final int INVALID = -1;

    public static final int NONE = 0;

    public static final int STORAGE = 1;

    public static final int BACKUP = 2;

    public static final int COMPUTE = 4;

    private NodeCapabilities() {}

    /**
     * Indicates if the specified capabilities are supported.
     *
     * @param p_capabilities The capabilities to check.
     * @return true, if <b>all</b> specified capabilities are supported; false otherwise.
     */
    public static boolean supports(int p_nodeCapabilities, int... p_capabilities) {

        int mask = toMask(p_capabilities);

        return (p_nodeCapabilities & mask) == mask;
    }

    /**
     * Creates a bitmask representing the specified capabilities.
     *
     * @param p_capabilities The capabilities.
     * @return A bitmask representing the specified capabilities.
     */
    public static int toMask(int... p_capabilities) {

        return Arrays.stream(p_capabilities).reduce(0, (a, b) -> a | b);
    }
}
