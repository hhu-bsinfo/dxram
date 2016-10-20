
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Reponse to the execute task request with status code.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class ExecuteTaskResponse extends AbstractResponse {
	/**
	 * Creates an instance of ExecuteTaskResponse.
	 * This constructor is used when receiving this message.
	 */
	public ExecuteTaskResponse() {
		super();
	}

	/**
	 * Creates an instance of ExecuteTaskResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request to respond to
	 */
	public ExecuteTaskResponse(final ExecuteTaskRequest p_request) {
		super(p_request, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE);
	}
}
