
package de.hhu.bsinfo.menet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages pending requests
 * @author Florian Klein
 *         09.03.2012
 */
public final class RequestMap {

	// Attributes
	private static Map<Integer, AbstractRequest> m_pendingRequests = new HashMap<>();

	private static Lock m_lock = new ReentrantLock(false);

	// Constructors
	/**
	 * Creates an instance of RequestStore
	 */
	private RequestMap() {}

	// Methods
	/**
	 * Put a Request in the store
	 * @param p_request
	 *            the Request
	 */
	protected static void put(final AbstractRequest p_request) {
		assert p_request != null;

		m_lock.lock();

		m_pendingRequests.put(p_request.getRequestID() & 0x00FFFFFF, p_request);

		m_lock.unlock();
	}

	/**
	 * Remove the Request of the given requestID from the store
	 * @param p_requestID
	 *            the requestID
	 * @return the removed Request
	 */
	protected static AbstractRequest remove(final int p_requestID) {
		AbstractRequest ret = null;

		m_lock.lock();

		ret = m_pendingRequests.remove(p_requestID);

		m_lock.unlock();

		return ret;
	}

	/**
	 * Returns the corresponding request
	 * @param p_resonse
	 *            the response
	 * @return the request
	 */
	static AbstractRequest getRequest(final AbstractResponse p_resonse) {
		AbstractRequest req = null;

		m_lock.lock();

		req = m_pendingRequests.get(p_resonse.getRequestID());

		m_lock.unlock();

		return req;
	}

	/**
	 * Fulfill a Request by the given Response
	 * @param p_response
	 *            the Response
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
