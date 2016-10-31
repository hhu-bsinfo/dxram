
package de.hhu.bsinfo.dxram.lock.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a LockRequest
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public class LockResponse extends AbstractResponse {

    /**
     * Creates an instance of LockResponse as a receiver.
     */
    public LockResponse() {
        super();
    }

    /**
     * Creates an instance of LockResponse as a sender.
     * @param p_request
     *            Corresponding request to this response.
     * @param p_statusCode
     *            Status code for locking the chunk.
     */
    public LockResponse(final LockRequest p_request, final byte p_statusCode) {
        super(p_request, LockMessages.SUBTYPE_LOCK_RESPONSE);

        setStatusCode(p_statusCode);
    }

}
