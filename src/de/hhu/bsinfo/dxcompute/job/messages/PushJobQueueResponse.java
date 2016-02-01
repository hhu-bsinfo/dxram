package de.hhu.bsinfo.dxcompute.job.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

public class PushJobQueueResponse extends AbstractResponse {
	/**
	 * Creates an instance of PushJobQueueResponse.
	 * This constructor is used when receiving this message.
	 */
	public PushJobQueueResponse() {
		super();
	}

	/**
	 * Creates an instance of PushJobQueueResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 * @param p_statusCode
	 *            Status code of the operation.
	 */
	public PushJobQueueResponse(final PushJobQueueRequest p_request, final byte p_statusCode) {
		super(p_request, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_RESPONSE);

		setStatusCode(p_statusCode);
	}
}
