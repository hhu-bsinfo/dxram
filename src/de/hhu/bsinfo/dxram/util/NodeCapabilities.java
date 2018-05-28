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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntBinaryOperator;
import java.util.stream.Collectors;

/**
 * Helper class for creating and testing individual capabilities.
 *
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class NodeCapabilities {

    public static final int INVALID = -1;

    public static final int NONE = 0;

    public static final int STORAGE = 1;

    public static final int BACKUP_SRC = 2;

    public static final int BACKUP_DST = 4;

    public static final int COMPUTE = 8;

    public static final String INVALID_STRING = "INVALID";

    public static final String NONE_STRING = "NONE";

    public static final String STORAGE_STRING = "STORAGE";

    public static final String BACKUP_SRC_STRING = "BACKUP_SRC";

    public static final String BACKUP_DST__STRING = "BACKUP_DST";

    public static final String COMPUTE__STRING = "COMPUTE";

    /**
     * Integer masking operator.
     */
    private static final IntBinaryOperator MASK_OPERATOR = (a, b) -> a | b;

    private NodeCapabilities() {}

    /**
     * Indicates if the specified capabilities are supported.
     *
     * @param p_capabilities The capabilities to check.
     * @return true, if <b>all</b> specified capabilities are supported; false otherwise.
     */
    public static boolean supportsAll(int p_nodeCapabilities, int... p_capabilities) {

        int mask = toMask(p_capabilities);

        return (p_nodeCapabilities & mask) == mask;
    }

    /**
     * Indicates if at least one of the specified capabilities is supported.
     *
     * @param p_capabilities The capabilities to check.
     * @return true, if <b>at least one</b> of the specified capabilities is supported; false otherwise.
     */
    public static boolean supports(int p_nodeCapabilities, int... p_capabilities) {

        int mask = toMask(p_capabilities);

        return (p_nodeCapabilities & mask) != NONE;
    }

    /**
     * Creates a bitmask representing the specified capabilities.
     *
     * @param p_capabilities The capabilities.
     * @return A bitmask representing the specified capabilities.
     */
    public static int toMask(int... p_capabilities) {

        return Arrays.stream(p_capabilities).reduce(0, MASK_OPERATOR);
    }

    /**
     * Creates a human-readable representation for the specified capabilities.
     *
     * @param p_capabilities A bitmask representing some capabilities.
     * @return A String representing the capabilities.
     */
    public static String toString(int p_capabilities) {

        List<String> capabilities = new ArrayList<>();

        BitSet bitSet = BitSet.valueOf(new long[]{p_capabilities});

        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {

            if (i == Integer.MAX_VALUE) {
                break;
            }

            int capability = 1 << i;

            switch (capability) {
                case NONE:
                    capabilities.add(NONE_STRING);
                    break;
                case STORAGE:
                    capabilities.add(STORAGE_STRING);
                    break;
                case BACKUP_SRC:
                    capabilities.add(BACKUP_SRC_STRING);
                    break;
                case BACKUP_DST:
                    capabilities.add(BACKUP_DST__STRING);
                    break;
                case COMPUTE:
                    capabilities.add(COMPUTE__STRING);
                    break;
            }
        }

        return capabilities.stream().collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * Creates a bitmask representing the specified capability.
     *
     * @param p_capability The capability.
     * @return A bitmask representing the capability.
     */
    public static int fromString(String p_capability) {

        switch (p_capability) {
            case INVALID_STRING:
                return INVALID;
            case NONE_STRING:
                return NONE;
            case STORAGE_STRING:
                return STORAGE;
            case BACKUP_SRC_STRING:
                return BACKUP_SRC;
            case BACKUP_DST__STRING:
                return BACKUP_DST;
            case COMPUTE__STRING:
                return COMPUTE;
        }

        return INVALID;
    }

    /**
     * Creates a bitmask representing the specified capabilities.
     *
     * <pre>
     * <u>Example</u>
     * {@code
     * int capabilities = NodeCapabilities.fromStringArray("[STORAGE, BACKUP_SRC]");
     * }
     * </pre>
     *
     * @param p_capabilities The formatted capabilities.
     * @return A bitmask representing the capability.
     */
    public static int fromStringArray(String p_capabilities) {

        if (!p_capabilities.startsWith("[") || !p_capabilities.endsWith("]")) {

            return NodeCapabilities.INVALID;
        }

        String[] capabilities = p_capabilities.substring(1, p_capabilities.length() - 1).replaceAll(" ", "").split(",");

        return NodeCapabilities.fromStrings(capabilities);
    }

    /**
     * Creates a bitmask representing all specified capabilities.
     *
     * @implNote This implementation filters out all invalid capabilities.
     * @param p_capabilities The capabilities.
     * @return A bitmask representing all specified capabilities.
     */
    public static int fromStrings(String... p_capabilities) {

        return Arrays.stream(p_capabilities)
                .mapToInt(NodeCapabilities::fromString)
                .filter(capability -> capability != INVALID)
                .reduce(0, MASK_OPERATOR);
    }
}
