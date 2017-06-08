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

package de.hhu.bsinfo.ethnet;

/**
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.05.2017
 */
class MessageStore {

    // Attributes
    private AbstractMessage[] m_buffer;
    private int m_size;

    private int m_posFront;
    private int m_posBack;

    /**
     * Creates an instance of MessageStore
     *
     * @param p_size
     *     the maximum number of messages to store
     */
    MessageStore(final int p_size) {
        m_size = p_size;
        m_buffer = new AbstractMessage[m_size];

        m_posFront = 0;
        m_posBack = 0;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return m_posFront == m_posBack;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     *
     * @return whether the ring-buffer is full or not
     */
    public boolean isFull() {
        return (m_posBack + 1) % m_size == m_posFront % m_size;
    }

    /**
     * Returns the number of pending buffers.
     *
     * @return the number of pending buffers
     */
    protected int size() {
        return m_posBack - m_posFront;
    }

    /**
     * Adds a message at the end of the buffer.
     *
     * @param p_message
     *     the message
     * @return whether the job was added or not
     */
    boolean pushMessage(final AbstractMessage p_message) {
        if ((m_posBack + 1) % m_size == m_posFront % m_size) {
            // Return without adding the message if queue is full
            return false;
        }

        m_buffer[m_posBack % m_size] = p_message;
        m_posBack++;

        return true;
    }

    /**
     * Gets a message from the beginning of the buffer.
     *
     * @return the message or null if empty
     */
    AbstractMessage popMessage() {
        AbstractMessage ret;

        if (m_posFront == m_posBack) {
            // Ring-buffer is empty.
            return null;
        }

        ret = m_buffer[m_posFront % m_size];
        m_posFront++;

        return ret;
    }
}
