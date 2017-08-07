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

package de.hhu.bsinfo.dxnet.core;

import java.lang.reflect.Constructor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles mapping from type and subtype to message class. Every message class has to be registered here before it can be used.
 * Message type 0 is dedicated to package intern used message classes.
 *
 * @author Marc Ewert, marc.ewert@hhu.de, 21.10.14
 */
public final class MessageDirectory {

    // Attributes
    private Constructor<?>[][] m_constructors = new Constructor[0][0];
    private ReentrantLock m_lock = new ReentrantLock(false);
    private int m_timeOut;

    /**
     * MessageDirectory is not designated to be instantiable
     *
     * @param p_requestTimeOut
     *         the request time out in ms
     */
    public MessageDirectory(final int p_requestTimeOut) {
        m_timeOut = p_requestTimeOut;
    }

    /**
     * Registers a Message Type for receiving
     *
     * @param p_type
     *         the type of the Message
     * @param p_subtype
     *         the subtype of the Message
     * @param p_class
     *         Message class
     * @return True if successful, false if the specified type and subtype are already in use.
     */
    public boolean register(final byte p_type, final byte p_subtype, final Class<?> p_class) {
        Constructor<?>[][] constructors = m_constructors;
        Constructor<?> constructor;

        m_lock.lock();
        try {
            constructor = p_class.getDeclaredConstructor();
        } catch (final NoSuchMethodException e) {
            m_lock.unlock();
            throw new IllegalArgumentException("Class " + p_class.getCanonicalName() + " has no default constructor", e);
        }

        if (contains(p_type, p_subtype)) {
            // everything's fine if the same message type for the same constructor
            // is registered multiple times
            if (constructors[p_type][p_subtype].equals(constructor)) {
                m_lock.unlock();
                return true;
            }

            m_lock.unlock();
            return false;
        }

        // enlarge array
        if (constructors.length <= p_type) {
            final Constructor<?>[][] newArray = new Constructor[(byte) (p_type + 1)][];
            System.arraycopy(constructors, 0, newArray, 0, constructors.length);
            constructors = newArray;
            m_constructors = constructors;
        }

        // create new sub array when it is not existing until now
        if (constructors[p_type] == null) {
            constructors[p_type] = new Constructor<?>[p_subtype + 1];
        }

        // enlarge subtype array
        if (constructors[p_type].length <= p_subtype) {
            final Constructor<?>[] newArray = new Constructor[p_subtype + 1];
            System.arraycopy(constructors[p_type], 0, newArray, 0, constructors[p_type].length);
            constructors[p_type] = newArray;
        }

        constructors[p_type][p_subtype] = constructor;
        m_lock.unlock();
        return true;
    }

    /**
     * Creates a Message instance for the type and subtype
     *
     * @param p_type
     *         the type of the Message
     * @param p_subtype
     *         the subtype of the Message
     * @return a new Message instance
     */
    public Message getInstance(final byte p_type, final byte p_subtype) {
        Message ret;
        long time;
        Constructor<?> constructor;

        constructor = getConstructor(p_type, p_subtype);

        // Try again in a loop, if constructor was not registered. Stop if request timeout is reached as answering later has no effect
        if (constructor == null) {
            time = System.currentTimeMillis();
            while (constructor == null && System.currentTimeMillis() < time + m_timeOut) {
                m_lock.lock();
                constructor = getConstructor(p_type, p_subtype);
                m_lock.unlock();
            }
        }

        if (constructor == null) {
            throw new NetworkRuntimeException("Could not create message instance: Message type (" + p_type + ':' + p_subtype + ") not registered");
        }

        try {
            ret = (Message) constructor.newInstance();
        } catch (final Exception e) {
            throw new NetworkRuntimeException("Could not create message instance", e);
        }

        return ret;
    }

    /**
     * Lookup, if a specific message type is already registered
     *
     * @param p_type
     *         the type of the Message
     * @param p_subtype
     *         the subtype of the Message
     * @return true if registered
     */
    private boolean contains(final byte p_type, final byte p_subtype) {
        boolean result;
        final Constructor<?>[][] constructors = m_constructors;

        result = constructors.length > p_type && !(constructors[p_type] == null || constructors[p_type].length <= p_subtype) &&
                constructors[p_type][p_subtype] != null;

        return result;
    }

    /**
     * Returns the constructor for a message class by its type and subtype
     *
     * @param p_type
     *         the type of the Message
     * @param p_subtype
     *         the subtype of the Message
     * @return message class constructor
     */
    private Constructor<?> getConstructor(final byte p_type, final byte p_subtype) {
        Constructor<?> result = null;

        if (contains(p_type, p_subtype)) {
            result = m_constructors[p_type][p_subtype];
        }

        return result;
    }
}
