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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.NetworkResponseCancelledException;
import de.hhu.bsinfo.dxnet.NetworkResponseDelayedException;

/**
 * Represents a Request
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class Request extends Message {
    private static final Logger LOGGER = LogManager.getFormatterLogger(Request.class.getSimpleName());

    // optimized value determined by experiments
    private static final long COUNTER_BASE = 1024;
    private static AtomicInteger ms_threadsWaiting = new AtomicInteger(0);

    // Attributes
    private volatile boolean m_fulfilled;
    private volatile boolean m_aborted;

    private boolean m_ignoreTimeout;

    private volatile Response m_response;

    // Constructors

    /**
     * Creates an instance of Request
     */
    protected Request() {
        super();

        m_response = null;
    }

    /**
     * Creates an instance of Request
     *
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     */
    protected Request(final short p_destination, final byte p_type, final byte p_subtype) {
        this(p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE);
    }

    /**
     * Creates an instance of Request
     *
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_exclusivity
     *         whether this request type allows parallel execution
     */
    protected Request(final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity) {
        super(p_destination, p_type, p_subtype, p_exclusivity);

        m_response = null;
    }

    /**
     * Set the ignore timeout option
     *
     * @param p_ignoreTimeout
     *         if true the request ignores the network timeout
     */
    public final void setIgnoreTimeout(final boolean p_ignoreTimeout) {
        m_ignoreTimeout = p_ignoreTimeout;
    }

    /**
     * Get the requestID
     *
     * @return the requestID
     */
    public final int getRequestID() {
        return getMessageID();
    }

    /**
     * Get the Response
     *
     * @return the Response
     */
    public final Response getResponse() {
        return m_response;
    }

    /**
     * Aborts waiting on response. Is called on failure detection
     */
    public final void abort() {
        m_aborted = true;
    }

    /**
     * Get the Response
     *
     * @param <T>
     *         the Response type
     * @param p_class
     *         the Class of the Response
     * @return the Response
     */
    public final <T extends Response> T getResponse(final Class<T> p_class) {
        T ret = null;

        assert p_class != null;

        if (m_response != null && p_class.isAssignableFrom(m_response.getClass())) {
            ret = p_class.cast(m_response);
            m_response.setCorrespondingRequest(this);
        } else {
            throw new NetworkRuntimeException("Can't get response to request " + this + ", not available (maybe due to sending failed request or timeout when receiving resposne?)");
        }

        return ret;
    }

    /**
     * Wait until the Request is fulfilled or aborted
     *
     * @param p_timeoutMs
     *         Max amount of time to wait for response.
     */
    public final void waitForResponse(final int p_timeoutMs) throws NetworkException {
        long cur = System.nanoTime();
        long deadline = cur + ((long) p_timeoutMs) * 1000 * 1000;

        ms_threadsWaiting.incrementAndGet();
        int counter = 0;

        while (!m_fulfilled) {

            if (m_aborted) {
                ms_threadsWaiting.decrementAndGet();

                // #if LOGGER >= DEBUG
                LOGGER.debug("Response for request %s , aborted, latency %f ms", toString(), (System.nanoTime() - cur) / 1000.0 / 1000.0);
                // #endif /* LOGGER >= DEBUG */

                throw new NetworkResponseCancelledException(getDestination());
            }

            if (!m_ignoreTimeout) {
                if (System.nanoTime() > deadline) {
                    ms_threadsWaiting.decrementAndGet();

                    // #if LOGGER >= DEBUG
                    LOGGER.debug("Response for request %s , delayed, latency %f ms", toString(), (System.nanoTime() - cur) / 1000.0 / 1000.0);
                    // #endif /* LOGGER >= DEBUG */

                    throw new NetworkResponseDelayedException(getDestination());
                }
            }

            // wait a bit, but increase waiting frequency with number of threads to reduce cpu load
            // but keep a higher cpu load to ensure low latency for less threads
            // (latency will increase with many threads anyway)
            if (counter > COUNTER_BASE / ms_threadsWaiting.get()) {
                counter = 0;
                LockSupport.parkNanos(1);
            } else {
                counter++;
            }
        }

        ms_threadsWaiting.decrementAndGet();

        // #if LOGGER >= TRACE
        LOGGER.trace("Request %s fulfilled, response %s, latency %f ms", toString(), m_response, (System.nanoTime() - cur) / 1000.0 / 1000.0);
        // #endif /* LOGGER >= TRACE */
    }

    /**
     * Fulfill the Request
     *
     * @param p_response
     *         the Response
     */
    final void fulfill(final Response p_response) {
        if (p_response == null) {
            throw new RuntimeException("Fullfilling request " + this + " with NULL response");
        }

        m_response = p_response;
        m_fulfilled = true;
    }

    @Override
    public void reuse() {
        super.reuse();

        m_response = null;
        m_fulfilled = false;
        m_aborted = false;
    }

}
