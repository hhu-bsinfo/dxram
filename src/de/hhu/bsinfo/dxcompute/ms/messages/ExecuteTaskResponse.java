
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

public class ExecuteTaskResponse extends AbstractResponse {
	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public ExecuteTaskResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public ExecuteTaskResponse(final ExecuteTaskRequest p_request) {
		super(p_request, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE);
	}
}
