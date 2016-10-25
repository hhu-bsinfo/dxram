
package de.hhu.bsinfo.ethnet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages pending requests
 *
 * @author Florian Klein
 *         09.03.2012
 */
public final class RequestMap {

	private static final Logger LOGGER = LogManager.getFormatterLogger(RequestMap.class.getSimpleName());

	// Attributes
	private static AbstractRequest[] m_pendingRequests;
	private static Lock m_lock;

	// Constructors

	/**
	 * Creates an instance of RequestStore
	 */
	private RequestMap() {
	}

	// Methods

	/**
	 * Initializes the request map
	 *
	 * @param p_size the number of entries in request map
	 */
	protected static void initialize(final int p_size) {
		m_pendingRequests = new AbstractRequest[p_size];
		m_lock = new ReentrantLock(false);
	}

	/**
	 * Put a Request in the store
	 *
	 * @param p_request the Request
	 */
	protected static void put(final AbstractRequest p_request) {
		int index;

		assert p_request != null;

		m_lock.lock();

		index = p_request.getRequestID() % m_pendingRequests.length;
		if (m_pendingRequests[index] != null) {
			// #if LOGGER >= ERROR
			LOGGER.error("Request for idx=%d still registered! Request Map might be too small", index);
			// #endif /* LOGGER >= ERROR */
		}
		m_pendingRequests[index] = p_request;

		m_lock.unlock();
	}

	/**
	 * Remove the Request of the given requestID from the store
	 *
	 * @param p_requestID the requestID
	 * @return the removed Request
	 */
	public static AbstractRequest remove(final int p_requestID) {
		int index;

		AbstractRequest ret = null;

		m_lock.lock();

		index = p_requestID % m_pendingRequests.length;
		ret = m_pendingRequests[index];
		m_pendingRequests[index] = null;

		m_lock.unlock();

		return ret;
	}

	/**
	 * Returns the corresponding request
	 *
	 * @param p_resonse the response
	 * @return the request
	 */
	static AbstractRequest getRequest(final AbstractResponse p_resonse) {
		AbstractRequest req = null;

		m_lock.lock();

		req = m_pendingRequests[p_resonse.getRequestID() % m_pendingRequests.length];

		m_lock.unlock();

		return req;
	}

	/**
	 * Fulfill a Request by the given Response
	 *
	 * @param p_response the Response
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

}
