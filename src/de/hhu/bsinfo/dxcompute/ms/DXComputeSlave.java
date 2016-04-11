
package de.hhu.bsinfo.dxcompute.ms;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

public class DXComputeSlave extends DXComputeMSBase implements MessageReceiver {

	private short m_masterNodeId = NodeID.INVALID_ID;

	private volatile AbstractTaskPayload m_task;
	private Lock m_executeTaskLock = new ReentrantLock(false);

	public DXComputeSlave(final DXRAM p_dxram, final int p_computeGroupId) {
		super(p_dxram, p_computeGroupId);

		m_networkService.registerReceiver(SlaveJoinResponse.class, this);
		m_networkService.registerReceiver(ExecuteTaskRequest.class, this);

		start();
	}

	@Override
	public void run() {

		while (true) {
			switch (m_state) {
				case STATE_SETUP:
					stateSetup();
					break;
				case STATE_IDLE:
					stateIdle();
					break;
				case STATE_EXECUTE:
					stateExecute();
					break;
				default:
					assert 1 == 2;
					break;
			}
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == MasterSlaveMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST:
						incomingExecuteTaskRequest((ExecuteTaskRequest) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	private void stateSetup() {
		// bootstrap: get master node id from nameservice
		if (m_masterNodeId == NodeID.INVALID_ID) {
			long tmp = m_nameserviceService.getChunkID(m_nameserviceMasterNodeIdKey);
			if (tmp == -1) {
				m_loggerService.error(getClass(),
						"Setting up slave, cannot find nameservice entry for master node id for key "
								+ m_nameserviceMasterNodeIdKey + " of compute group " + m_computeGroupId);
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}

				return;
			}

			// cut off the creator id part, which identifies our master node
			m_masterNodeId = ChunkID.getCreatorID(tmp);
		}

		SlaveJoinRequest request = new SlaveJoinRequest(m_masterNodeId);
		NetworkErrorCodes err = m_networkService.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_loggerService.error(getClass(),
					"Sending join request to master " + Integer.toHexString(m_masterNodeId) + " failed: " + err);
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}
		}

		SlaveJoinResponse response = (SlaveJoinResponse) request.getResponse();
		if (response.getStatusCode() != 0) {
			// master is busy, retry
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}
		} else {
			m_loggerService.info(getClass(), "Successfully joined compute group " + m_computeGroupId + " with master "
					+ Integer.toHexString(m_masterNodeId));
			m_state = State.STATE_IDLE;
		}
	}

	private void stateIdle() {
		if (m_task != null) {
			m_state = State.STATE_EXECUTE;
		} else {
			// do nothing
			Thread.yield();
		}
	}

	private void stateExecute() {
		m_loggerService.info(getClass(),
				"Starting execution of task " + m_task);

		int result = m_task.execute(m_dxram);

		m_loggerService.info(getClass(),
				"Executing task " + m_task + " done.");

		m_loggerService.info(getClass(),
				"Syncing with master " + Integer.toHexString(m_masterNodeId) + " ...");

		// TODO hit barrier here and send the result of the execution with the barrier sync

		m_loggerService.debug(getClass(),
				"Syncing done.");

		m_state = State.STATE_IDLE;
	}

	private void incomingExecuteTaskRequest(final ExecuteTaskRequest p_message) {
		if (m_executeTaskLock.tryLock() && m_state == State.STATE_IDLE) {
			m_task = p_message.getTask();
			if (m_task == null) {
				// TODO return status 2 that we could not create a proper task object from the message
				// this could be a result that we are missing a registered class for this
			}

			m_executeTaskLock.unlock();
		} else {
			// TODO return status 1, that we cannot execute it, invalid state
		}
	}
}
