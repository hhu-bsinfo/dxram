
package de.hhu.bsinfo.dxcompute.ms;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.coord.BarrierSlaveInternal;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

public class ComputeSlave extends ComputeMSBase implements MessageReceiver {

	private short m_masterNodeId = NodeID.INVALID_ID;

	private volatile int m_taskFinishedBarrierIdentifier = -1;
	private volatile AbstractTaskPayload m_task;
	private Lock m_executeTaskLock = new ReentrantLock(false);

	private BarrierSlaveInternal m_barrier;

	public ComputeSlave(final int p_computeGroupId, final DXRAMServiceAccessor p_serviceAccessor,
			final NetworkComponent p_network,
			final LoggerComponent p_logger, final NameserviceComponent p_nameservice,
			final AbstractBootComponent p_boot) {
		super(ComputeRole.SLAVE, p_computeGroupId, p_serviceAccessor, p_network, p_logger, p_nameservice, p_boot);

		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST, SlaveJoinRequest.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE, SlaveJoinResponse.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST, ExecuteTaskRequest.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE, ExecuteTaskResponse.class);

		m_network.register(SlaveJoinResponse.class, this);
		m_network.register(ExecuteTaskRequest.class, this);

		m_barrier = new BarrierSlaveInternal(m_network, m_logger);

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
	public void shutdown() {
		// TODO
		if (isAlive()) {
			// TODO dedicated thread shutdown
		} else {

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
		m_logger.info(getClass(), "Setting up slave for compute group " + m_computeGroupId + ")");

		// bootstrap: get master node id from nameservice
		if (m_masterNodeId == NodeID.INVALID_ID) {
			long tmp = m_nameservice.getChunkID(m_nameserviceMasterNodeIdKey, 0);
			if (tmp == -1) {
				m_logger.error(getClass(),
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
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending join request to master " + NodeID.toHexString(m_masterNodeId) + " failed: " + err);
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}
		} else {
			SlaveJoinResponse response = (SlaveJoinResponse) request.getResponse();
			if (response.getStatusCode() != 0) {
				// master is busy, retry
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			} else {
				m_logger.info(getClass(),
						"Successfully joined compute group " + m_computeGroupId + " with master "
								+ NodeID.toHexString(m_masterNodeId));
				m_state = State.STATE_IDLE;
				m_logger.debug(getClass(), "Entering idle state");
			}
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
		m_logger.info(getClass(),
				"Starting execution of task " + m_task);

		m_executeTaskLock.lock();
		int result = m_task.execute(getServiceAccessor());
		m_task = null;
		m_executeTaskLock.unlock();

		m_logger.info(getClass(),
				"Syncing with master " + NodeID.toHexString(m_masterNodeId) + " ...");

		// set idle state before sync to avoid race condition with master sending
		// new tasks right after the sync
		m_state = State.STATE_IDLE;

		// sync back with master and pass result to it via barrier
		m_barrier.execute(m_masterNodeId, m_taskFinishedBarrierIdentifier, result);

		m_logger.info(getClass(),
				"Syncing done.");

		m_logger.debug(getClass(), "Entering idle state");
	}

	private void incomingExecuteTaskRequest(final ExecuteTaskRequest p_message) {
		ExecuteTaskResponse response = new ExecuteTaskResponse(p_message);
		AbstractTaskPayload task = null;

		if (m_executeTaskLock.tryLock() && m_state == State.STATE_IDLE) {
			m_taskFinishedBarrierIdentifier = p_message.getBarrierIdentifier();
			task = p_message.getTask();
			if (task == null) {
				// could not create proper task object from message
				// this could be a result that we are missing a registered class for this
				response.setStatusCode((byte) 2);
			} else {
				// success
				response.setStatusCode((byte) 0);
			}

			m_executeTaskLock.unlock();
		} else {
			// cannot execute task, invalid state
			response.setStatusCode((byte) 1);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response for executing task to "
					+ NodeID.toHexString(p_message.getSource()) + " failed: " + err);
		} else {
			// assign and start execution if non null
			m_task = task;
		}
	}
}
