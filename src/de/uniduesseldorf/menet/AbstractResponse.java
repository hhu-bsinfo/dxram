
package de.uniduesseldorf.menet;

/**
 * Represents a Response to a Request
 * @author Florian Klein
 *         09.03.2012
 */
public abstract class AbstractResponse extends AbstractMessage {

	private AbstractRequest m_correspondingRequest = null;
	
	// Constructors
	/**
	 * Creates an instance of Response
	 */
	public AbstractResponse() {
		super();
	}

	/**
	 * Creates an instance of Response
	 * @param p_request
	 *            the corresponding Request
	 */
	public AbstractResponse(final AbstractRequest p_request) {
		super(p_request.getMessageID(), p_request.getDestination(), p_request.getSource(), p_request.getType(), DEFAULT_SUBTYPE);
	
		m_correspondingRequest = p_request;
	}

	/**
	 * Creates an instance of Response
	 * @param p_request
	 *            the corresponding Request
	 * @param p_subtype
	 *            the message subtype
	 */
	public AbstractResponse(final AbstractRequest p_request, final byte p_subtype) {
		super(p_request.getMessageID(), p_request.getDestination(), p_request.getSource(), p_request.getType(), p_subtype);
	
		m_correspondingRequest = p_request;
	}

	// Getters
	/**
	 * Get the responseID
	 * @return the responseID
	 */
	public final long getResponseID() {
		return getMessageID();
	}

	/**
	 * Get the requestID
	 * @return the requestID
	 */
	public final long getRequestID() {
		return getMessageID();
	}

	protected AbstractRequest getCorrespondingRequest() {
		return m_correspondingRequest;
	}
}
