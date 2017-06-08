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
    private static AbstractRequest[] ms_pendingRequests;

    private static ReentrantReadWriteLock ms_lock = new ReentrantReadWriteLock(false);

    // Constructors

    /**
     * Creates an instance of RequestStore
     */
    private RequestMap() {
    }

    // Methods

    /**
     * Remove the Request of the given requestID from the store
     *
     * @param p_requestID
     *     the requestID
     * @return the removed Request
     */
    public static AbstractRequest remove(final int p_requestID) {
        AbstractRequest ret;
        int index;

        ms_lock.readLock().lock();
        index = p_requestID % ms_pendingRequests.length;
        ret = ms_pendingRequests[index];
        ms_pendingRequests[index] = null;
        ms_lock.readLock().unlock();

        return ret;
    }

    /**
     * Removes all requests send to given NodeID
     *
     * @param p_nodeID
     *     the NodeID
     */
    public static void removeAll(final short p_nodeID) {
        AbstractRequest request;

        ms_lock.writeLock().lock();
        for (int i = 0; i < ms_pendingRequests.length; i++) {
            request = ms_pendingRequests[i];
            if (request != null && request.getDestination() == p_nodeID) {
                request.abort();
            }
        }
        ms_lock.writeLock().unlock();
    }

    /**
     * Returns the corresponding request
     *
     * @param p_resonse
     *     the response
     * @return the request
     */
    static AbstractRequest getRequest(final AbstractResponse p_resonse) {
        AbstractRequest ret;

        ms_lock.readLock().lock();
        ret = ms_pendingRequests[p_resonse.getRequestID() % ms_pendingRequests.length];
        ms_lock.readLock().unlock();

        return ret;
    }

    /**
     * Fulfill a Request by the given Response
     *
     * @param p_response
     *     the Response
     */
    static void fulfill(final AbstractResponse p_response) {
        AbstractRequest request;

        if (p_response != null) {
            request = remove(p_response.getRequestID());

            if (request != null) {
                request.fulfill(p_response);
            }
        }
    }

    /**
     * Put a Request in the store
     *
     * @param p_request
     *     the Request
     */
    static void put(final AbstractRequest p_request) {
        int index;

        ms_lock.readLock().lock();
        index = p_request.getRequestID() % ms_pendingRequests.length;
        if (ms_pendingRequests[index] != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Request for idx=%d still registered! Request Map might be too small", index);
            // #endif /* LOGGER >= ERROR */
        }
        ms_pendingRequests[index] = p_request;
        ms_lock.readLock().unlock();
    }

    /**
     * Initializes the request map
     *
     * @param p_size
     *     the number of entries in request map
     */
    static void initialize(final int p_size) {
        ms_pendingRequests = new AbstractRequest[p_size];
    }

}
