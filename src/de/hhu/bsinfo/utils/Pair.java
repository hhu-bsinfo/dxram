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

package de.hhu.bsinfo.utils;

/**
 * Class for bundling two attributes
 *
 * @param <K>
 *     second attribute
 * @param <T>
 *     first attribute
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.02.2016
 */
public class Pair<T, K> {
    public T m_first;
    public K m_second;

    /**
     * Default constructor
     */
    public Pair() {

    }

    /**
     * Creates an instance of Pair
     *
     * @param p_first
     *     first attribute
     * @param p_second
     *     second attribute
     */
    public Pair(final T p_first, final K p_second) {
        m_first = p_first;
        m_second = p_second;
    }

    /**
     * Returns the first attribute
     *
     * @return the first attribute
     */
    public T first() {
        return m_first;
    }

    /**
     * Returns the second attribute
     *
     * @return the second attribute
     */
    public K second() {
        return m_second;
    }
}
