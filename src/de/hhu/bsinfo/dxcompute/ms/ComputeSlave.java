
package de.hhu.bsinfo.dxcompute.ms;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Implementation of a slave. The slave waits for tasks for execution from the master
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
class ComputeSlave extends AbstractComputeMSBase implements MessageReceiver {

	private short m_masterNodeId = NodeID.INVALID_ID;

	private volatile AbstractTaskPayload m_task;
	private Lock m_executeTaskLock = new ReentrantLock(false);

	private int m_masterExecutionBarrierId;

	/**
	 * Constructor
	 *
	 * @param p_computeGroupId  Compute group id the instance is assigned to.
	 * @param p_pingIntervalMs  Ping interval in ms to check back with the compute group if still alive.
	 * @param p_serviceAccessor Service accessor for tasks.
	 * @param p_network         NetworkComponent
	 * @param p_logger          LoggerComponent
	 * @param p_nameservice     NameserviceComponent
	 * @param p_boot            BootComponent
	 * @param p_lookup          LookupComponent
	 */
	ComputeSlave(final short p_computeGroupId, final long p_pingIntervalMs,
			final DXRAMServiceAccessor p_serviceAccessor,
			final NetworkComponent p_network,
			final LoggerComponent p_logger, final NameserviceComponent p_nameservice,
			final AbstractBootComponent p_boot,
			final LookupComponent p_lookup) {
		super(ComputeRole.SLAVE, p_computeGroupId, p_pingIntervalMs, p_serviceAccessor, p_network, p_logger,
				p_nameservice, p_boot, p_lookup);

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

		m_masterExecutionBarrierId = BarrierID.INVALID_ID;

		start();
	}

	@Override
	public void run() {

		boolean loop = true;
		while (loop) {
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
				case STATE_TERMINATE:
					loop = false;
					break;
				default:
					assert false;
					break;
			}
		}
	}

	@Override
	public void shutdown() {
		if (isAlive()) {
			m_state = State.STATE_TERMINATE;
			try {
				join();
			} catch (final InterruptedException ignored) {
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

	/**
	 * Setup state of the slave. Connect to the master of the compute group assigend to.
	 */
	private void stateSetup() {
		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Setting up slave for compute group " + m_computeGroupId + ")");
		// #endif /* LOGGER >= DEBUG */

		// bootstrap: get master node id from nameservice
		if (m_masterNodeId == NodeID.INVALID_ID) {
			long tmp = m_nameservice.getChunkID(m_nameserviceMasterNodeIdKey, 0);
			if (tmp == -1) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"Setting up slave, cannot find nameservice entry for master node id for key "
								+ m_nameserviceMasterNodeIdKey + " of compute group " + m_computeGroupId);
				// #endif /* LOGGER >= ERROR */
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException ignored) {
				}

				return;
			}

			// cut off the creator id part, which identifies our master node
			m_masterNodeId = ChunkID.getCreatorID(tmp);
		}

		SlaveJoinRequest request = new SlaveJoinRequest(m_masterNodeId);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Sending join request to master " + NodeID.toHexString(m_masterNodeId) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException ignored) {
			}

			// trigger a full retry. might happen that the master node has changed
			m_masterNodeId = NodeID.INVALID_ID;
		} else {
			SlaveJoinResponse response = (SlaveJoinResponse) request.getResponse();
			if (response.getStatusCode() != 0) {
				// master is busy, retry
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException ignored) {
				}
			} else {
				// #if LOGGER >= INFO
				m_logger.info(getClass(),
						"Successfully joined compute group " + m_computeGroupId + " with master "
								+ NodeID.toHexString(m_masterNodeId));
				// #endif /* LOGGER >= INFO */

				m_masterExecutionBarrierId = response.getExecutionBarrierId();
				m_state = State.STATE_IDLE;

				// #if LOGGER >= DEBUG
				m_logger.debug(getClass(), "Entering idle state");
				// #endif /* LOGGER >= DEBUG */
			}
		}
	}

	/**
	 * Idle state. Connected to the master of the compute group. Ping and wait for instructions.
	 */
	private void stateIdle() {
		if (m_task != null) {
			m_state = State.STATE_EXECUTE;
		} else {
			// check periodically if master is still available
			if (m_lastPingMs + m_pingIntervalMs < System.currentTimeMillis()) {
				if (!m_boot.isNodeOnline(m_masterNodeId)) {
					// master is gone, go back to sign on
					// #if LOGGER >= INFO
					m_logger.info(getClass(),
							"Master " + NodeID.toHexString(m_masterNodeId) + " went offline, logout.");
					// #endif /* LOGGER >= INFO */

					m_masterNodeId = NodeID.INVALID_ID;
					m_state = State.STATE_SETUP;
					return;
				}

				m_lastPingMs = System.currentTimeMillis();

				// #if LOGGER == TRACE
				m_logger.trace(getClass(), "Pinging master " + NodeID.toHexString(m_masterNodeId) + ": online.");
				// #endif /* LOGGER == TRACE */
			}

			try {
				Thread.sleep(10);
			} catch (final InterruptedException ignored) {
			}
		}
	}

	/**
	 * Execute state. Execute the assigned task.
	 */
	private void stateExecute() {
		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Starting execution of task " + m_task);
		// #endif /* LOGGER >= INFO */

		m_executeTaskLock.lock();
		int result = m_task.execute(getServiceAccessor());
		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Execution finished, return code: " + result);
		// #endif /* LOGGER >= INFO */

		m_task = null;
		m_executeTaskLock.unlock();

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(),
				"Syncing with master " + NodeID.toHexString(m_masterNodeId) + " ...");
		// #endif /* LOGGER >= DEBUG */

		// set idle state before sync to avoid race condition with master sending
		// new tasks right after the sync
		m_state = State.STATE_IDLE;

		// sync back with master and pass result to it via barrier
		m_lookup.barrierSignOn(m_masterExecutionBarrierId, result);

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Syncing done.");
		// #endif /* LOGGER >= DEBUG */

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Entering idle state");
		// #endif /* LOGGER >= DEBUG */
	}

	/**
	 * Handle an incoming ExecuteTaskRequest
	 *
	 * @param p_message ExecuteTaskRequest
	 */
	private void incomingExecuteTaskRequest(final ExecuteTaskRequest p_message) {
		ExecuteTaskResponse response = new ExecuteTaskResponse(p_message);
		AbstractTaskPayload task = null;

		if (m_executeTaskLock.tryLock() && m_state == State.STATE_IDLE) {
			task = p_message.getTaskPayload();
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
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Sending response for executing task to "
					+ NodeID.toHexString(p_message.getSource()) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
		} else {
			if (task != null) {
				// assign and start execution if non null
				task.setComputeGroupId(m_computeGroupId);
				m_task = task;
			}
		}
	}
}
