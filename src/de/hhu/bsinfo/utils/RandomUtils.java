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

package de.hhu.bsinfo.utils;

import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Utility functions to generate random numbers
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public final class RandomUtils {

    /**
     * Utils class
     */
    private RandomUtils() {

    }

    /**
     * Get a random size from a size range
     *
     * @param p_start
     *         Start of size range (including)
     * @param p_end
     *         End of size range (including)
     * @return Random size in bytes
     */
    public static int getRandomSize(final StorageUnit p_start, final StorageUnit p_end) {
        return (int) getRandomValue(p_start.getBytes(), p_end.getBytes());
    }

    /**
     * Get a random number in the range of 0 to a specific end
     *
     * @param p_end
     *         End (including)
     * @return Random int
     */
    public static int getRandomValue(final int p_end) {
        return getRandomValue(0, p_end);
    }

    /**
     * Get a random number from a specified range
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (including)
     * @return Random int
     */
    public static int getRandomValue(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }

    /**
     * Get a random number from a specified range
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (excluding)
     * @return Random int
     */
    public static int getRandomValueExclEnd(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start) + p_start);
    }

    /**
     * Get a random number from a specified range
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (including)
     * @return Random int
     */
    public static long getRandomValue(final long p_start, final long p_end) {
        return (long) (Math.random() * (p_end - p_start + 1) + p_start);
    }
}
