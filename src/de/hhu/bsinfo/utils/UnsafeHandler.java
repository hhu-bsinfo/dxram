package de.hhu.bsinfo.utils;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Enables direct access to the main memory
 *
 * @author Florian Klein, florian.klein@hhu.de, 22.07.2013
 */
public final class UnsafeHandler {

    // Attributes
    private Unsafe m_unsafe;

    // Constructors

    /**
     * Creates an instance of UnsafeHandler
     */
    private UnsafeHandler() {
        Field field;

        try {
            field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            m_unsafe = (Unsafe) field.get(null);
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
    public Unsafe getUnsafe() {
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

    // Classes

    /**
     * Implements the SingeltonPattern for PropertyHelper
     *
     * @author Florian Klein
     *         22.07.2013
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
