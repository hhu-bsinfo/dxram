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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages pending requests
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public final class RequestMap {
    private static final Logger LOGGER = LogManager.getFormatterLogger(RequestMap.class.getSimpleName());

    // Attributes
    private Request[] m_pendingRequests;

    private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock(false);

    // Constructors

    /**
     * Creates an instance of RequestStore
     *
     * @param p_size
     *         the number of entries in request map
     */
    public RequestMap(final int p_size) {
        m_pendingRequests = new Request[p_size];
    }

    /**
     * Put a Request in the store
     *
     * @param p_request
     *         the Request
     */
    public void put(final Request p_request) {
        int index;

        m_lock.readLock().lock();
        index = p_request.getRequestID() % m_pendingRequests.length;
        if (m_pendingRequests[index] != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Request %s for idx=%d still registered! Request Map might be too small", m_pendingRequests[index], index);
            // #endif /* LOGGER >= ERROR */
        }
        m_pendingRequests[index] = p_request;
        m_lock.readLock().unlock();
    }

    /**
     * Remove the Request of the given requestID from the store
     *
     * @param p_requestID
     *         the requestID
     * @return the removed Request
     */
    public Request remove(final int p_requestID) {
        Request ret;
        int index;

        m_lock.readLock().lock();
        index = p_requestID % m_pendingRequests.length;
        ret = m_pendingRequests[index];
        m_pendingRequests[index] = null;
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Removes all requests send to given NodeID
     *
     * @param p_nodeID
     *         the NodeID
     */
    public void removeAll(final short p_nodeID) {
        Request request;

        m_lock.writeLock().lock();
        for (int i = 0; i < m_pendingRequests.length; i++) {
            request = m_pendingRequests[i];
            if (request != null && request.getDestination() == p_nodeID) {
                request.abort();
                m_pendingRequests[i] = null;
            }
        }
        m_lock.writeLock().unlock();
    }

    /**
     * Returns the corresponding request
     *
     * @param p_response
     *         the response
     * @return the request
     */
    Request getRequest(final Response p_response) {
        Request ret;

        m_lock.readLock().lock();
        ret = m_pendingRequests[p_response.getRequestID() % m_pendingRequests.length];
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Fulfill a Request by the given Response
     *
     * @param p_response
     *         the Response
     */
    void fulfill(final Response p_response) {
        Request request;

        if (p_response != null) {
            request = remove(p_response.getRequestID());

            if (request != null) {
                request.fulfill(p_response);
            }
        }
    }
}
