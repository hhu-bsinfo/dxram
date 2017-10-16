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
 * Represents a Response to a Request
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class Response extends Message {

    private Request m_correspondingRequest;

    // Constructors

    /**
     * Creates an instance of Response
     */
    protected Response() {
        super();
    }

    /**
     * Creates an instance of Response
     *
     * @param p_request
     *         the corresponding Request
     * @param p_subtype
     *         the message subtype
     */
    protected Response(final Request p_request, final byte p_subtype) {
        super(p_request.getMessageID(), p_request.getSource(), p_request.getType(), p_subtype);

        m_correspondingRequest = p_request;
    }

    /**
     * Returns the corresponding request
     *
     * @return the corresponding request
     */
    protected Request getCorrespondingRequest() {
        return m_correspondingRequest;
    }

    /**
     * Sets the corresponding request
     *
     * @param p_correspondingRequest
     *         the corresponding request
     */
    void setCorrespondingRequest(final Request p_correspondingRequest) {
        m_correspondingRequest = p_correspondingRequest;
    }

    /**
     * Get the requestID
     *
     * @return the requestID
     */
    final int getRequestID() {
        return getMessageID();
    }

    /**
     * Reset/Initialize all state and assign new message ID.
     */
    public void reuse(final Request p_request, final byte p_subtype) {
        set(p_request.getMessageID(), p_request.getSource(), p_request.getType(), p_subtype);

        m_correspondingRequest = p_request;
    }
}
