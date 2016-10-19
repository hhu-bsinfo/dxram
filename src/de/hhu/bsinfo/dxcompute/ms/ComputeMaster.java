
package de.hhu.bsinfo.dxcompute.ms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SignalMessage;
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
import de.hhu.bsinfo.utils.Pair;

/**
 * Implementation of a master. The master accepts tasks, pushes them to a queue and distributes them
 * to the conencted slaves for execution.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
class ComputeMaster extends AbstractComputeMSBase implements MessageReceiver {
	private static final int MAX_TASK_COUNT = 100;

	private Vector<Short> m_signedOnSlaves = new Vector<>();
	private Lock m_joinLock = new ReentrantLock(false);
	private ConcurrentLinkedQueue<Task> m_tasks = new ConcurrentLinkedQueue<>();
	private AtomicInteger m_taskCount = new AtomicInteger(0);
	private int m_executeBarrierIdentifier;
	private int m_executionBarrierId;

	private volatile int m_tasksProcessed;

	/**
	 * Constructor
	 *
	 * @param p_computeGroupId  Compute group id the instance is assigned to.
	 * @param p_pingIntervalMs  Ping interval in ms to check back with the compute group if still alive.
	 * @param p_serviceAccessor Accessor to services for compute tasks.
	 * @param p_network         NetworkComponent
	 * @param p_logger          LoggerComponent
	 * @param p_nameservice     NameserviceComponent
	 * @param p_boot            BootComponent
	 * @param p_lookup          LookupComponent
	 */
	ComputeMaster(final short p_computeGroupId, final long p_pingIntervalMs,
			final DXRAMServiceAccessor p_serviceAccessor,
			final NetworkComponent p_network,
			final LoggerComponent p_logger, final NameserviceComponent p_nameservice,
			final AbstractBootComponent p_boot,
			final LookupComponent p_lookup) {
		super(ComputeRole.MASTER, p_computeGroupId, p_pingIntervalMs, p_serviceAccessor, p_network, p_logger,
				p_nameservice, p_boot, p_lookup);

		p_network.register(SlaveJoinRequest.class, this);

		m_executionBarrierId = m_lookup.barrierAllocate(1);

		start();
	}

	/**
	 * Get a list of currently connected salves.
	 *
	 * @return List of currently connected slaves (node ids).
	 */
	ArrayList<Short> getConnectedSlaves() {
		@SuppressWarnings("unchecked")
		Vector<Short> tmp = (Vector<Short>) m_signedOnSlaves.clone();
		ArrayList<Short> ret = new ArrayList<>(tmp.size());
		ret.addAll(tmp);

		return ret;
	}

	/**
	 * Submit a task to this master.
	 *
	 * @param p_task Task to submit.
	 * @return True if submission was successful, false if the max number of tasks queued is reached.
	 */
	boolean submitTask(final Task p_task) {
		if (m_taskCount.get() < MAX_TASK_COUNT) {
			m_tasks.add(p_task);
			m_taskCount.incrementAndGet();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Get the number of tasks currently in the queue.
	 *
	 * @return Number of tasks in the queue.
	 */
	int getNumberOfTasksInQueue() {
		return m_taskCount.get();
	}

	/**
	 * Get the total amount of tasks processed so far.
	 *
	 * @return Number of tasks processed.
	 */
	int getTotalTasksProcessed() {
		return m_tasksProcessed;
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
				case STATE_ERROR_DIE:
					stateErrorDie();
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
		// shutdown main compute thread
		m_state = State.STATE_TERMINATE;
		try {
			join();
		} catch (final InterruptedException ignored) {
		}

		// invalidate entry in nameservice
		m_nameservice.register(-1, m_nameserviceMasterNodeIdKey);
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == MasterSlaveMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST:
						incomingSlaveJoinRequest((SlaveJoinRequest) p_message);
						break;
					case MasterSlaveMessages.SUBTYPE_SIGNAL:
						incomingSignalMessage((SignalMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	/**
	 * Setup state. Register node id in the nameservice to allow slaves to discover this master.
	 */
	private void stateSetup() {
		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Setting up master of compute group " + m_computeGroupId);
		// #endif /* LOGGER >= INFO */

		// check first, if there is already a master registered for this compute group
		long id = m_nameservice.getChunkID(m_nameserviceMasterNodeIdKey, 0);
		if (id != -1) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot setup master for compute group id " + m_computeGroupId + ", node "
					+ NodeID.toHexString(ChunkID.getCreatorID(id)) + " is already master of group");
			// #endif /* LOGGER >= ERROR */
			m_state = State.STATE_ERROR_DIE;
			return;
		}

		// setup bootstrapping for other slaves
		// use the nameservice to store our node id
		m_nameservice.register(ChunkID.getChunkID(m_boot.getNodeID(), ChunkID.INVALID_ID),
				m_nameserviceMasterNodeIdKey);

		m_state = State.STATE_IDLE;

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Entering idle state");
		// #endif /* LOGGER >= DEBUG */
	}

	/**
	 * Idle state. Wait for slaves to sign on and for tasks to be submitted. Also ping and check if slaves
	 * are still available and remove them from the group if not.
	 */
	private void stateIdle() {
		if (m_taskCount.get() > 0) {
			if (m_signedOnSlaves.size() < 1) {
				// #if LOGGER >= WARN
				m_logger.warn(getClass(), "Got " + m_taskCount.get() + " tasks queued but no slaves");
				// #endif /* LOGGER >= WARN */
				try {
					Thread.sleep(2000);
				} catch (final InterruptedException ignored) {
				}
			} else {
				m_state = State.STATE_EXECUTE;
			}
		} else {
			// check if we have to ping the slaves to check if they are still online
			if (m_lastPingMs + m_pingIntervalMs < System.currentTimeMillis()) {
				checkAllSlavesOnline();
			}

			// do nothing
			try {
				Thread.sleep(10);
			} catch (final InterruptedException ignored) {
			}
		}
	}

	/**
	 * Execute state. Execute a task from the queue. Send it to the slaves, wait for completion of all slaves.
	 */
	private void stateExecute() {

		// get next task
		m_taskCount.decrementAndGet();
		Task task = m_tasks.poll();
		TaskPayload taskPayload = task.getPayload();
		if (taskPayload == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot proceed with task " + task + ", missing payload.");
			// #endif /* LOGGER >= ERROR */
			m_state = State.STATE_IDLE;
			return;
		}

		// check if enough slaves are available for the task to run
		if (task.getPayload().getNumRequiredSlaves() != TaskPayload.NUM_REQUIRED_SLAVES_ARBITRARY
				&& task.getPayload().getNumRequiredSlaves() > m_signedOnSlaves.size()) {
			// #if LOGGER >= INFO
			m_logger.info(getClass(),
					"Not enough slaves available for task " + task + " waiting...");
			// #endif /* LOGGER >= INFO */

			while (task.getPayload().getNumRequiredSlaves() > m_signedOnSlaves.size()) {
				// #if LOGGER >= DEBUG
				m_logger.debug(getClass(),
						"Not enough slaves available for task " + task + " waiting (" + m_signedOnSlaves.size() + "/"
								+ task.getPayload().getNumRequiredSlaves() + ")...");
				// #endif /* LOGGER >= DEBUG */

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}

				// bad but might happen that a slave goes offline
				checkAllSlavesOnline();
			}
		}

		// lock joining of further slaves
		m_joinLock.lock();

		// #if LOGGER >= INFO
		m_logger.info(getClass(),
				"Starting execution of task " + task + " with " + m_signedOnSlaves.size() + " slaves.");
		// #endif /* LOGGER >= INFO */

		short[] slaves = new short[m_signedOnSlaves.size()];
		for (int i = 0; i < slaves.length; i++) {
			slaves[i] = m_signedOnSlaves.get(i);
		}

		task.notifyListenersExecutionStarts();

		// send task to slaves
		short numberOfSlavesOnExecution = 0;
		// avoid clashes with other compute groups, but still alter the flag on every next sync
		m_executeBarrierIdentifier = (m_executeBarrierIdentifier + 1) % 2 + m_computeGroupId * 2;
		for (short slave : slaves) {
			TaskContextData ctxData =
					new TaskContextData(m_computeGroupId, numberOfSlavesOnExecution, slaves);

			// pass barrier identifier for syncing after task along
			ExecuteTaskRequest request =
					new ExecuteTaskRequest(slave, m_executeBarrierIdentifier, ctxData, taskPayload);

			NetworkErrorCodes err = m_network.sendSync(request);
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"Sending task to slave " + NodeID.toHexString(slave) + " failed: " + err);
				// #endif /* LOGGER >= ERROR */
				// remove slave from list
				m_signedOnSlaves.remove(slave);
				continue;
			}

			ExecuteTaskResponse response = (ExecuteTaskResponse) request.getResponse();
			if (response.getStatusCode() != 0) {
				// exclude slave from execution
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Slave " + NodeID.toHexString(slave) + " response "
						+ response.getStatusCode() + " on execution of task " + task
						+ " excluding from current execution");
				// #endif /* LOGGER >= ERROR */
			} else {
				numberOfSlavesOnExecution++;
			}
		}

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(),
				"Syncing with " + numberOfSlavesOnExecution + "/" + m_signedOnSlaves.size() + " slaves...");
		// #endif /* LOGGER >= DEBUG */

		Pair<short[], long[]> result = m_lookup.barrierSignOn(m_executionBarrierId, -1);

		int[] returnCodes;
		if (result != null) {
			// #if LOGGER >= DEBUG
			m_logger.debug(getClass(), "Syncing done.");
			// #endif /* LOGGER >= DEBUG */

			// grab return codes from barrier
			returnCodes = new int[slaves.length];

			// sort them to match the indices of the slave list
			for (int j = 0; j < slaves.length; j++) {
				for (int i = 0; i < result.first().length; i++) {
					if (result.first()[i] == slaves[j]) {
						returnCodes[j] = (int) result.second()[i];
						break;
					}
				}
			}
		} else {
			returnCodes = new int[slaves.length];
			for (int i = 0; i < returnCodes.length; i++) {
				returnCodes[i] = -1;
			}
		}

		m_tasksProcessed++;

		task.notifyListenersExecutionCompleted(returnCodes);

		m_state = State.STATE_IDLE;
		// allow further slaves to join
		m_joinLock.unlock();

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Entering idle state");
		// #endif /* LOGGER >= DEBUG */
	}

	/**
	 * Error state. Entered if an error happened and we can't recover.
	 */
	private void stateErrorDie() {
		// #if LOGGER >= ERROR
		m_logger.error(getClass(), "Master error state");
		// #endif /* LOGGER >= ERROR */
		try {
			Thread.sleep(1000);
		} catch (final InterruptedException ignored) {
		}
	}

	/**
	 * Check online status of all slaves (once).
	 */
	private void checkAllSlavesOnline() {
		// check if slaves are still alive
		List<Short> onlineNodesList = m_boot.getIDsOfOnlineNodes();

		m_joinLock.lock();
		Iterator<Short> it = m_signedOnSlaves.iterator();
		while (it.hasNext()) {
			short slave = it.next();
			if (!onlineNodesList.contains(slave)) {
				// #if LOGGER >= INFO
				m_logger.info(getClass(),
						"Slave " + NodeID.toHexString(slave) + " is not available anymore, removing.");
				// #endif /* LOGGER >= INFO */

				it.remove();
			}
		}
		m_joinLock.unlock();

		m_lastPingMs = System.currentTimeMillis();
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Pinging slaves, " + m_signedOnSlaves.size() + " online.");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Handle a SlaveJoinRequest
	 *
	 * @param p_message SlaveJoinRequest
	 */
	private void incomingSlaveJoinRequest(final SlaveJoinRequest p_message) {
		if (m_joinLock.tryLock()) {
			if (m_signedOnSlaves.contains(p_message.getSource())) {
				// #if LOGGER >= WARN
				m_logger.warn(getClass(), "Joining slave, already joined: "
						+ NodeID.toHexString(p_message.getSource()));
				// #endif /* LOGGER >= WARN */
			} else {
				m_signedOnSlaves.add(p_message.getSource());

				// expand barrier, +1 for the master
				m_lookup.barrierChangeSize(m_executionBarrierId, m_signedOnSlaves.size() + 1);
			}

			SlaveJoinResponse response = new SlaveJoinResponse(p_message, m_executionBarrierId);
			response.setStatusCode((byte) 0);
			NetworkErrorCodes err = m_network.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Sending response to join request of slave "
						+ NodeID.toHexString(p_message.getSource()) + "failed: " + err);
				// #endif /* LOGGER >= ERROR */
				// remove slave
				m_signedOnSlaves.remove(p_message.getSource());
			} else {
				// #if LOGGER >= INFO
				m_logger.info(getClass(), "Slave (" + (m_signedOnSlaves.size() - 1) + ") "
						+ NodeID.toHexString(p_message.getSource()) + " has joined");
				// #endif /* LOGGER >= INFO */
			}

			m_joinLock.unlock();
		} else {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "Cannot join slave, master not in idle state.");
			// #endif /* LOGGER == TRACE */

			// send response that joining is not possible currently
			SlaveJoinResponse response = new SlaveJoinResponse(p_message, BarrierID.INVALID_ID);
			response.setStatusCode((byte) 1);
			NetworkErrorCodes err = m_network.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Sending response to join request of slave "
						+ NodeID.toHexString(p_message.getSource()) + "failed: " + err);
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	/**
	 * Handle a SignalMessage
	 *
	 * @param p_message SignalMessage
	 */
	private void incomingSignalMessage(final SignalMessage p_message) {
		switch (p_message.getSignal()) {
			case SIGNAL_ABORT: {
				// the slave requested aborting the currently running task
				// send an abort to all other slaves as well
				for (short slaveNodeId : m_signedOnSlaves) {
					NetworkErrorCodes err =
							m_network.sendMessage(new SignalMessage(slaveNodeId, p_message.getSignal()));
					if (err != NetworkErrorCodes.SUCCESS) {
						// #if LOGGER >= ERROR
						m_logger.error(getClass(), "Sending signal to slave "
								+ NodeID.toHexString(p_message.getSource()) + "failed: " + err);
						// #endif /* LOGGER >= ERROR */
					}
				}

				break;
			}
			default: {
				// #if LOGGER >= ERROR

				m_logger.error(getClass(), "Unhandled signal " + p_message.getSignal() + " from peer " +
						NodeID.toHexString(p_message.getSource()));
				// #endif /* LOGGER >= ERROR */
				break;
			}
		}
	}
}
