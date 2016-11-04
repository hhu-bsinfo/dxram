package de.hhu.bsinfo.ethnet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private static Lock ms_lock;

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
        int index;

        AbstractRequest ret;

        ms_lock.lock();

        index = p_requestID % ms_pendingRequests.length;
        ret = ms_pendingRequests[index];
        ms_pendingRequests[index] = null;

        ms_lock.unlock();

        return ret;
    }

    /**
     * Returns the corresponding request
     *
     * @param p_resonse
     *     the response
     * @return the request
     */
    static AbstractRequest getRequest(final AbstractResponse p_resonse) {
        AbstractRequest req;

        ms_lock.lock();

        req = ms_pendingRequests[p_resonse.getRequestID() % ms_pendingRequests.length];

        ms_lock.unlock();

        return req;
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

        assert p_request != null;

        ms_lock.lock();

        index = p_request.getRequestID() % ms_pendingRequests.length;
        if (ms_pendingRequests[index] != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Request for idx=%d still registered! Request Map might be too small", index);
            // #endif /* LOGGER >= ERROR */
        }
        ms_pendingRequests[index] = p_request;

        ms_lock.unlock();
    }

    /**
     * Initializes the request map
     *
     * @param p_size
     *     the number of entries in request map
     */
    static void initialize(final int p_size) {
        ms_pendingRequests = new AbstractRequest[p_size];
        ms_lock = new ReentrantLock(false);
    }

}
