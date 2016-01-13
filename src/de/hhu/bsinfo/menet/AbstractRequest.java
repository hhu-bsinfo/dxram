
package de.hhu.bsinfo.menet;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import de.hhu.bsinfo.utils.Contract;

/**
 * Represents a Request
 * @author Florian Klein 09.03.2012
 */
public abstract class AbstractRequest extends AbstractMessage {

	// Constants
	public static final long WAITING_TIMEOUT = 20;

	// Attributes
	private volatile boolean m_fulfilled;
	private AbstractAction<AbstractRequest> m_fulfillAction;

	private volatile boolean m_aborted;
	private AbstractAction<AbstractRequest> m_abortAction;

	private boolean m_ignoreTimeout;

	private Semaphore m_wait;

	private AbstractResponse m_response;

	// Constructors
	/**
	 * Creates an instance of Request
	 */
	protected AbstractRequest() {
		super();

		m_fulfilled = false;
		m_fulfillAction = null;

		m_aborted = false;
		m_abortAction = null;

		m_wait = new Semaphore(0, false);

		m_response = null;
	}

	/**
	 * Creates an instance of Request
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 */
	public AbstractRequest(final short p_destination, final byte p_type) {
		this(p_destination, p_type, DEFAULT_SUBTYPE);
	}

	/**
	 * Creates an instance of Request
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 */
	public AbstractRequest(final short p_destination, final byte p_type, final byte p_subtype) {
		super(p_destination, p_type, p_subtype);

		m_fulfilled = false;
		m_fulfillAction = null;

		m_aborted = false;
		m_abortAction = null;

		m_wait = new Semaphore(0, false);

		m_response = null;
	}

	// Getters
	/**
	 * Checks if the network timeout for the request should be ignored
	 * @return true if the timeout should be ignored, false otherwise
	 */
	public final boolean isIgnoreTimeout() {
		return m_ignoreTimeout;
	}

	/**
	 * Get the requestID
	 * @return the requestID
	 */
	public final long getRequestID() {
		return getMessageID();
	}

	/**
	 * Get the Response
	 * @return the Response
	 */
	public final AbstractResponse getResponse() {
		return m_response;
	}

	/**
	 * Get the Response
	 * @param <T>
	 *            the Response type
	 * @param p_class
	 *            the Class of the Response
	 * @return the Response
	 */
	public final <T extends AbstractResponse> T getResponse(final Class<T> p_class) {
		T ret = null;

		Contract.checkNotNull(p_class, "no class given");

		if (m_response != null && p_class.isAssignableFrom(m_response.getClass())) {
			ret = p_class.cast(m_response);
		}

		return ret;
	}

	/**
	 * Checks if the Request is fulfilled
	 * @return true if the Request is fulfilled, false otherwise
	 */
	public final boolean isFulfilled() {
		return m_fulfilled;
	}

	/**
	 * Checks if the Request is aborted
	 * @return true if the Request is aborted, false otherwise
	 */
	public final boolean isAborted() {
		return m_aborted;
	}

	// Setters
	/**
	 * Set the ignore timeout option
	 * @param p_ignoreTimeout
	 *            if true the request ignores the network timeout
	 */
	public final void setIgnoreTimeout(final boolean p_ignoreTimeout) {
		m_ignoreTimeout = p_ignoreTimeout;
	}

	// Methods
	/**
	 * Wait until the Request is fulfilled or aborted
	 * @returns False if message timed out, true if response received.
	 */
	public final boolean waitForResponses() {
		boolean success = true;
		long timeStart;
		long timeNow;

		timeStart = System.currentTimeMillis();

		while (!m_fulfilled && !m_aborted) {
			timeNow = System.currentTimeMillis();
			if (timeNow - timeStart > 1200 && !m_ignoreTimeout) {
				RequestStatistic.getInstance().requestTimeout(getRequestID(), getClass());
				success = false;
			}
			try {
				m_wait.tryAcquire(WAITING_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (final InterruptedException e) {}
		}
		
		return success;
	}

	/**
	 * Registers the Action, that will be executed, if the Request is fulfilled
	 * @param p_action
	 *            the Action
	 */
	public final void registerFulfillAction(final AbstractAction<AbstractRequest> p_action) {
		m_fulfillAction = p_action;
	}

	/**
	 * Fulfill the Request
	 * @param p_response
	 *            the Response
	 */
	final void fulfill(final AbstractResponse p_response) {
		Contract.checkNotNull(p_response, "no response given");

		RequestStatistic.getInstance().responseReceived(getRequestID(), getClass());

		m_response = p_response;

		m_fulfilled = true;
		m_wait.release();

		if (m_fulfillAction != null) {
			m_fulfillAction.execute(this);
		}
	}

	/**
	 * Registers the Action, that will be executed, if the Request is aborted
	 * @param p_action
	 *            the Action
	 */
	public final void registerAbortAction(final AbstractAction<AbstractRequest> p_action) {
		m_abortAction = p_action;
	}

	/**
	 * Aborts the Request
	 */
	public final void abort() {
		RequestMap.remove(getRequestID());

		RequestStatistic.getInstance().requestAborted(getRequestID(), getClass());

		m_aborted = true;
		m_wait.release();

		if (m_abortAction != null) {
			m_abortAction.execute(this);
		}
	}

	@Override
	protected final void beforeSend() {
		RequestMap.put(this);
	}

	@Override
	protected final void afterSend() {
		RequestStatistic.getInstance().requestSend(getRequestID());
	}

}
