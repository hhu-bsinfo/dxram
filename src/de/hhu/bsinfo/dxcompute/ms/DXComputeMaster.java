
package de.hhu.bsinfo.dxcompute.ms;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

public class DXComputeMaster extends DXComputeMSBase implements MessageReceiver {
	private static final int MAX_TASK_COUNT = 100;

	private ArrayList<Short> m_signedOnSlaves = new ArrayList<Short>();
	private Lock m_joinLock = new ReentrantLock(false);
	private ConcurrentLinkedQueue<AbstractTaskPayload> m_tasks;
	private AtomicInteger m_taskCount = new AtomicInteger(0);

	public DXComputeMaster(final DXRAM p_dxram, final int p_computeGroupId) {
		super(p_dxram, p_computeGroupId);

		m_networkService.registerReceiver(SlaveJoinRequest.class, this);

		start();
	}

	public boolean submitTask(final AbstractTaskPayload p_task) {
		if (m_taskCount.get() < MAX_TASK_COUNT) {
			m_tasks.add(p_task);
			m_taskCount.incrementAndGet();
			return true;
		} else {
			return false;
		}
	}

	public int getNumberOfTasksInQueue() {
		return m_taskCount.get();
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
					case MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST:
						incomingSlaveJoinRequest((SlaveJoinRequest) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	private void stateSetup() {
		m_loggerService.info(getClass(), "Setting up master of compute group " + m_computeGroupId + ")");

		// setup bootstrapping for other slaves
		// use the nameservice to store our node id
		m_nameserviceService.register(ChunkID.getChunkID(m_bootService.getNodeID(), ChunkID.INVALID_ID),
				m_nameserviceMasterNodeIdKey);

		m_state = State.STATE_IDLE;
	}

	private void stateIdle() {
		if (m_taskCount.get() > 0) {
			m_state = State.STATE_EXECUTE;
		} else {
			// do nothing
			Thread.yield();
		}
	}

	private void stateExecute() {
		// lock joining of further slaves
		m_joinLock.lock();

		// get next task
		AbstractTaskPayload task = m_tasks.poll();

		m_loggerService.info(getClass(),
				"Starting execution of task " + task + " with " + m_signedOnSlaves.size() + " slaves.");

		// send task to slaves
		for (Short slaveId : m_signedOnSlaves) {
			ExecuteTaskRequest request = new ExecuteTaskRequest(slaveId, task);

			NetworkErrorCodes err = m_networkService.sendSync(request);
			if (err != NetworkErrorCodes.SUCCESS) {
				// TODO remove slave from signed on array?
				continue;
			}

			ExecuteTaskResponse response = (ExecuteTaskResponse) request.getResponse();
			if (response.getStatusCode() != 0) {
				// TODO error handling for codes 1 and 2
				// exclude slave from this execution?
			}
		}

		m_loggerService.info(getClass(),
				"Syncing with " + m_signedOnSlaves.size() + " slaves...");

		// TODO pass incrementing sync token with execution request for barrier
		// TODO hit barrier here

		m_loggerService.debug(getClass(),
				"Syncing done.");

		m_state = State.STATE_IDLE;

		// allow further slaves to join
		m_joinLock.unlock();
	}

	private void incomingSlaveJoinRequest(final SlaveJoinRequest p_message) {
		if (m_joinLock.tryLock()) {
			if (m_signedOnSlaves.contains(p_message.getSource())) {
				m_loggerService.warn(getClass(), "Joining slave, already joined: "
						+ Integer.toHexString(p_message.getSource()));
			} else {
				m_signedOnSlaves.add(p_message.getSource());
			}

			SlaveJoinResponse response = new SlaveJoinResponse(p_message);
			response.setStatusCode((byte) 0);
			NetworkErrorCodes err = m_networkService.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending response to join request of slave "
						+ Integer.toHexString(p_message.getSource()) + "failed: " + err);
				// remove slave
				m_signedOnSlaves.remove(p_message.getSource());
			} else {
				m_loggerService.info(getClass(), "Slave " + Integer.toHexString(p_message.getSource()) + " has joined");
			}

			m_joinLock.unlock();
		} else {
			m_loggerService.trace(getClass(), "Cannot join slave, master not in idle state.");

			// send response that joining is not possible currently
			SlaveJoinResponse response = new SlaveJoinResponse(p_message);
			response.setStatusCode((byte) 1);
			NetworkErrorCodes err = m_networkService.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending response to join request of slave "
						+ Integer.toHexString(p_message.getSource()) + "failed: " + err);
			}
		}
	}
}
