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

package de.hhu.bsinfo.ethnet;

/**
 * Represents a Response to a Request
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public abstract class AbstractResponse extends AbstractMessage {

    private AbstractRequest m_correspondingRequest;

    // Constructors

    /**
     * Creates an instance of Response
     */
    protected AbstractResponse() {
        super();
    }

    /**
     * Creates an instance of Response
     *
     * @param p_request
     *     the corresponding Request
     * @param p_subtype
     *     the message subtype
     */
    protected AbstractResponse(final AbstractRequest p_request, final byte p_subtype) {
        super(p_request.getMessageID(), p_request.getSource(), p_request.getType(), p_subtype);

        m_correspondingRequest = p_request;
    }

    // Getters

    /**
     * Get the responseID
     *
     * @return the responseID
     */
    public final int getResponseID() {
        return getMessageID();
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
     * Returns the corresponding request
     *
     * @return the corresponding request
     */
    protected AbstractRequest getCorrespondingRequest() {
        return m_correspondingRequest;
    }

    /**
     * Sets the corresponding request
     *
     * @param p_correspondingRequest
     *     the corresponding request
     */
    void setCorrespondingRequest(final AbstractRequest p_correspondingRequest) {
        m_correspondingRequest = p_correspondingRequest;
    }
}
