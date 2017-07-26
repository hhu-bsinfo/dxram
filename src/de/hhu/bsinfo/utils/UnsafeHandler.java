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

import java.lang.reflect.Field;

/**
 * Enables direct access to the main memory
 *
 * @author Florian Klein, florian.klein@hhu.de, 22.07.2013
 */
@SuppressWarnings("sunapi")
public final class UnsafeHandler {

    // Attributes
    private sun.misc.Unsafe m_unsafe;

    // Constructors

    /**
     * Creates an instance of UnsafeHandler
     */
    private UnsafeHandler() {
        Field field;

        try {
            field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            m_unsafe = (sun.misc.Unsafe) field.get(null);
        } catch (final Exception e) {
            throw new AssertionError(e);
        }
    }

    // Getters

    /**
     * Get the instance of the Unsafe class
     *
     * @return the instance of the Unsafe class
     */
    public sun.misc.Unsafe getUnsafe() {
        return m_unsafe;
    }

    // Methods

    /**
     * Get the instance of the UnsafeHandler
     *
     * @return the instance of the UnsafeHandler
     */
    public static UnsafeHandler getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Get the offset of the byte array within the object byte[]
     *
     * @return the byte array offset
     */
    public static int getArrayByteOffset() {
        return sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
    }

    // Classes

    /**
     * Implements the SingeltonPattern for PropertyHelper
     *
     * @author Florian Klein
     * 22.07.2013
     */
    private static final class Holder {

        // Constants
        private static final UnsafeHandler INSTANCE = new UnsafeHandler();

        // Constructors

        /**
         * Creates an instance of Holder
         */
        private Holder() {
        }

    }

}
