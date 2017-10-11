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

/**
 * Wrapper for caching an unfinished operation during import/export.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class UnfinishedImExporterOperation {
    private volatile int m_startIndex;
    private volatile long m_primitive;
    private volatile Object m_object;

    // Following attributes are relevant for importing, only
    private volatile Message m_unfinishedMessage;
    private volatile MessageHeader m_currentHeader;
    private volatile int m_bytesCopied;

    /**
     * Creates an instance of UnfinishedImExporterOperation
     */
    UnfinishedImExporterOperation() {
        m_primitive = 0;
        m_object = null;
        m_startIndex = 0;

        m_unfinishedMessage = null;
        m_currentHeader = null;
        m_bytesCopied = 0;
    }

    @Override
    public String toString() {
        return "m_startIndex " + m_startIndex + ", m_primitive " + m_primitive + ", m_object " + m_object;
    }

    /**
     * Get index within buffer of unfinished operation
     *
     * @return the index
     */
    int getIndex() {
        return m_startIndex;
    }

    /**
     * Get unfinished primitive (short, int, long, float or double)
     *
     * @return the primitive as long
     */
    long getPrimitive() {
        return m_primitive;
    }

    /**
     * Get unfinished object (e.g. byte array)
     *
     * @return the unfinished Object (data structure or message)
     */
    Object getObject() {
        return m_object;
    }

    /**
     * Get the number of bytes copied until the end of buffer was reached
     *
     * @return the number of bytes de-serialized
     */
    int getBytesCopied() {
        return m_bytesCopied;
    }

    /**
     * Get corresponding message
     *
     * @return the message
     */
    Message getMessage() {
        return m_unfinishedMessage;
    }

    /**
     * Get corresponding message header
     *
     * @return the message header
     */
    MessageHeader getMessageHeader() {
        return m_currentHeader;
    }

    /**
     * Returns whether this unfinished operation is empty or not
     *
     * @return whether this operation is empty or not
     */
    boolean isEmpty() {
        return m_currentHeader == null;
    }

    /**
     * Set index of unfinished operation
     *
     * @param p_index
     *         the index
     */
    void setIndex(final int p_index) {
        m_startIndex = p_index;
    }

    /**
     * Set unfinished primitive
     *
     * @param p_primitive
     *         the primitive (short, int, long, float or double)
     */
    void setPrimitive(final long p_primitive) {
        m_primitive = p_primitive;
    }

    /**
     * Set unfinished object
     *
     * @param p_object
     *         the object
     */
    void setObject(final Object p_object) {
        m_object = p_object;
    }

    /**
     * Set corresponding message
     *
     * @param p_unfinishedMessage
     *         the unfinished message
     */
    void setMessage(final Message p_unfinishedMessage) {
        m_unfinishedMessage = p_unfinishedMessage;
    }

    /**
     * Set corresponding message header
     *
     * @param p_messageHeader
     *         the message header
     */
    void setMessageHeader(final MessageHeader p_messageHeader) {
        m_currentHeader = p_messageHeader;
    }

    /**
     * Increment number of bytes copied
     *
     * @param p_bytesCopied
     *         the number of bytes copied
     */
    void incrementBytesCopied(final int p_bytesCopied) {
        m_bytesCopied += p_bytesCopied;
    }

    /**
     * Returns if the message was already created
     *
     * @return true if the message was created
     */
    boolean wasMessageCreated() {
        return m_unfinishedMessage != null;
    }

    /**
     * Reset instance
     */
    void reset() {
        m_primitive = 0;
        m_object = null;
        m_startIndex = 0;

        m_unfinishedMessage = null;
        m_currentHeader = null;
        m_bytesCopied = 0;
    }
}
