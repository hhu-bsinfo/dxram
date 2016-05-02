
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxcompute.ms.messages.*;
import de.hhu.bsinfo.dxcompute.ms.tasks.*;
import de.hhu.bsinfo.dxcompute.ms.tcmd.*;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * DXRAM service providing a master slave based distributed task execution framework for computation on DXRAM.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class MasterSlaveComputeService extends AbstractDXRAMService implements MessageReceiver, TaskListener {

	private NetworkComponent m_network;
	private NameserviceComponent m_nameservice;
	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;
	private TerminalComponent m_terminal;

	private AbstractComputeMSBase m_computeMSInstance;

	private ConcurrentMap<Integer, Task> m_remoteTasks = new ConcurrentHashMap<Integer, Task>();

	/**
	 * Get the compute of the current node.
	 *
	 * @return Compute role.
	 */
	public ComputeRole getComputeRole() {
		return m_computeMSInstance.getRole();
	}

	/**
	 * Get a list of all available master nodes.
	 *
	 * @return List of available master nodes with their compute group id
	 */
	public ArrayList<Pair<Short, Byte>> getMasters() {
		ArrayList<Pair<Short, Byte>> masters = new ArrayList<Pair<Short, Byte>>();

		// check the name service entries
		for (int i = 0; i <= AbstractComputeMSBase.MAX_COMPUTE_GROUP_ID; i++) {
			long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + i, 0);
			if (tmp != -1) {
				masters.add(new Pair<Short, Byte>(ChunkID.getCreatorID(tmp), (byte) i));
			}
		}

		return masters;
	}

	/**
	 * Get the status of the current master node.
	 *
	 * @return Status of the current master node or null if the current node is not a master.
	 */
	public StatusMaster getStatusMaster() {
		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(), "Cannot get status on non master node type");
			return null;
		}

		ArrayList<Short> slaves = ((ComputeMaster) m_computeMSInstance).getConnectedSlaves();
		int numTasksInQueue = ((ComputeMaster) m_computeMSInstance).getNumberOfTasksInQueue();
		AbstractComputeMSBase.State state = ((ComputeMaster) m_computeMSInstance).getComputeState();
		int tasksProcessed = ((ComputeMaster) m_computeMSInstance).getTotalTasksProcessed();

		return new StatusMaster(m_boot.getNodeID(), state, slaves, numTasksInQueue, tasksProcessed);
	}

	/**
	 * Get the status of a remote master node.
	 *
	 * @param p_computeGroupId Compute group id to get the master's status of
	 * @return Status of the remote master or null if remote node is not a master or getting the status failed.
	 */
	public StatusMaster getStatusMaster(final short p_computeGroupId) {
		if (m_computeMSInstance.getRole() == ComputeRole.MASTER) {
			ComputeMaster master = (ComputeMaster) m_computeMSInstance;
			if (master.getComputeGroupId() == p_computeGroupId) {
				return getStatusMaster();
			}
		}

		// get the node id of the master node of the group
		short masterNodeId = NodeID.INVALID_ID;
		{
			long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + p_computeGroupId, 0);
			if (tmp == -1) {
				m_logger.error(getClass(), "Cannot find master node of compute gropu id " + p_computeGroupId);
				return null;
			}
			masterNodeId = ChunkID.getCreatorID(tmp);
		}

		GetMasterStatusRequest request = new GetMasterStatusRequest(masterNodeId);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Getting status of master " + NodeID.toHexString(masterNodeId) + " failed: " + err);
			return null;
		}

		GetMasterStatusResponse response = (GetMasterStatusResponse) request.getResponse();
		if (response.getStatusCode() != 0) {
			m_logger.error(getClass(), "Cannot get status on non master node " + NodeID.toHexString(masterNodeId));
			return null;
		}

		return response.getStatusMaster();
	}

	/**
	 * Submit a task to this master.
	 *
	 * @param p_task Task to submit to this master.
	 * @return The task id assigned or -1 if the current node is not a master or submission failed.
	 */
	public long submitTask(final Task p_task) {
		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(), "Cannot submit task " + p_task + " on non master node type");
			return -1;
		}

		p_task.setNodeIdSubmitted(m_boot.getNodeID());
		p_task.m_serviceAccessor = getServiceAccessor();

		if (((ComputeMaster) m_computeMSInstance).submitTask(p_task)) {
			return p_task.getPayload().getPayloadId();
		} else {
			return -1;
		}
	}

	/**
	 * Submit a task to remote master node.
	 *
	 * @param p_task           Task to submit to another master node.
	 * @param p_computeGroupId Compute group to submit the task to.
	 * @return Task id assigned by the remote master node or -1 if the remote node is not a master or submission failed.
	 */
	public long submitTask(final Task p_task, final short p_computeGroupId) {
		if (m_computeMSInstance.getRole() == ComputeRole.MASTER) {
			ComputeMaster master = (ComputeMaster) m_computeMSInstance;
			if (master.getComputeGroupId() == p_computeGroupId) {
				return submitTask(p_task);
			}
		}

		p_task.setNodeIdSubmitted(m_boot.getNodeID());

		// get the node id of the master node of the group
		short masterNodeId = NodeID.INVALID_ID;
		{
			long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + p_computeGroupId, 0);
			if (tmp == -1) {
				m_logger.error(getClass(), "Cannot find master node of compute gropu id " + p_computeGroupId);
				return -1;
			}
			masterNodeId = ChunkID.getCreatorID(tmp);
		}

		SubmitTaskRequest request = new SubmitTaskRequest(masterNodeId, p_task.getPayload());
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending submit task request to node " + NodeID.toHexString(masterNodeId) + " failed: " + err);
			return -1;
		}

		SubmitTaskResponse response = (SubmitTaskResponse) request.getResponse();
		if (response.getStatusCode() != 0) {
			m_logger.error(getClass(), "Error submitting task, code " + response.getStatusCode());
			return -1;
		}

		// get assigned payload id
		p_task.getPayload().setComputeGroupId(response.getAssignedComputeGroupId());
		p_task.getPayload().setPayloadId(response.getAssignedPayloadId());

		// remember task for remote callbacks
		m_remoteTasks.put(p_task.getPayload().getPayloadId(), p_task);

		m_logger.info(getClass(), "Submitted task to compute group " + p_computeGroupId + " with master node "
				+ NodeID.toHexString(masterNodeId) + ": " + p_task);

		return p_task.getPayload().getPayloadId();
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		// only used for remote tasks to callback the node they were submitted on
		TaskExecutionStartedMessage message =
				new TaskExecutionStartedMessage(p_task.getNodeIdSubmitted(), p_task.getPayload().getPayloadId(),
						p_task.getPayload().getSlaveNodeIds());

		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending remote callback before execution to node " + p_task.getNodeIdSubmitted() + " failed.");
		}
	}

	@Override
	public void taskCompleted(final Task p_task) {
		// only used for remote tasks to callback the node they were submitted on
		TaskExecutionFinishedMessage message =
				new TaskExecutionFinishedMessage(p_task.getNodeIdSubmitted(), p_task.getPayload().getPayloadId(),
						p_task.getPayload().getExecutionReturnCodes());

		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending remote callback completed to node " + p_task.getNodeIdSubmitted() + " failed.");
		}

		// we don't have to remember this remote task anymore
		m_remoteTasks.remove(p_task.getPayload().getPayloadId());
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == MasterSlaveMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST:
						incomingSubmitTaskRequest((SubmitTaskRequest) p_message);
						break;
					case MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST:
						incomingGetMasterStatusRequest((GetMasterStatusRequest) p_message);
						break;
					case MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE:
						incomingTaskExecutionStartedMessage((TaskExecutionStartedMessage) p_message);
						break;
					case MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE:
						incomingTaskExecutionFinishedMessage((TaskExecutionFinishedMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {
		p_settings.setDefaultValue(MasterSlaveConfigurationValues.Service.ROLE);
		p_settings.setDefaultValue(MasterSlaveConfigurationValues.Service.COMPUTE_GROUP_ID);
		p_settings.setDefaultValue(MasterSlaveConfigurationValues.Service.PING_INTERVAL_MS);
	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		ComputeRole role =
				ComputeRole.toComputeRole(p_settings.getValue(MasterSlaveConfigurationValues.Service.ROLE));
		short computeGroupId = p_settings.getValue(MasterSlaveConfigurationValues.Service.COMPUTE_GROUP_ID);
		int pingIntervalMs = p_settings.getValue(MasterSlaveConfigurationValues.Service.PING_INTERVAL_MS);

		m_network = getComponent(NetworkComponent.class);
		m_nameservice = getComponent(NameserviceComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_boot = getComponent(AbstractBootComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		m_network.registerMessageType(MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST,
				SubmitTaskRequest.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE,
				SubmitTaskResponse.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST,
				GetMasterStatusRequest.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_RESPONSE,
				GetMasterStatusResponse.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE,
				TaskExecutionStartedMessage.class);
		m_network.registerMessageType(MasterSlaveMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE,
				TaskExecutionFinishedMessage.class);

		m_network.register(SubmitTaskRequest.class, this);
		m_network.register(GetMasterStatusRequest.class, this);
		m_network.register(TaskExecutionStartedMessage.class, this);
		m_network.register(TaskExecutionFinishedMessage.class, this);

		m_terminal.registerCommand(new TcmdMSGroupList());
		m_terminal.registerCommand(new TcmdMSComputeGroupStatus());
		m_terminal.registerCommand(new TcmdMSTaskSubmit());
		m_terminal.registerCommand(new TcmdMSTasks());
		m_terminal.registerCommand(new TcmdMSTaskListSubmit());

		switch (role) {
			case MASTER:
				m_computeMSInstance = new ComputeMaster(computeGroupId, pingIntervalMs, getServiceAccessor(),
						m_network, m_logger,
						m_nameservice, m_boot);
				break;
			case SLAVE:
				m_computeMSInstance =
						new ComputeSlave(computeGroupId, pingIntervalMs, getServiceAccessor(), m_network, m_logger,
								m_nameservice, m_boot);
				break;
			case NONE:
				m_computeMSInstance = new ComputeNone(getServiceAccessor(), m_network, m_logger,
						m_nameservice, m_boot);
				break;
			default:
				assert 1 == 2;
				break;
		}

		registerTaskPayloads();

		m_logger.info(getClass(), "Started compute node " + role + " with compute group id " + computeGroupId);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_network = null;
		m_nameservice = null;
		m_logger = null;

		m_computeMSInstance.shutdown();
		m_computeMSInstance = null;

		return true;
	}

	@Override
	protected boolean isServiceAccessor() {
		// we need this for the tasks
		return true;
	}

	/**
	 * Handle an incoming SubmitTaskRequest
	 *
	 * @param p_request SubmitTaskRequest
	 */
	private void incomingSubmitTaskRequest(final SubmitTaskRequest p_request) {
		SubmitTaskResponse response;

		m_logger.debug(getClass(), "Incoming remote submit task request " + p_request);

		// check if we were able to create an instance (missing task class registration)
		if (p_request.getTaskPayload() == null) {
			m_logger.error(getClass(), "Creating instance for task payload of request " + p_request
					+ " failed, most likely non registered task payload type");
			response = new SubmitTaskResponse(p_request, (short) -1, -1);
			response.setStatusCode((byte) 3);
			return;
		}

		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(),
					"Cannot submit remote task " + p_request.getTaskPayload() + " on non master node type");

			response = new SubmitTaskResponse(p_request, (short) -1, -1);
			response.setStatusCode((byte) 1);
		} else {
			Task task = new Task(p_request.getTaskPayload(), "RemoteTask" + p_request);
			task.setNodeIdSubmitted(p_request.getSource());
			task.m_serviceAccessor = getServiceAccessor();
			task.registerTaskListener(this);

			boolean ret = ((ComputeMaster) m_computeMSInstance).submitTask(task);

			if (ret) {
				m_remoteTasks.put(task.getPayload().getPayloadId(), task);

				response = new SubmitTaskResponse(p_request, task.getPayload().getComputeGroupId(),
						task.getPayload().getPayloadId());
				response.setStatusCode((byte) 0);
			} else {
				response = new SubmitTaskResponse(p_request, (short) -1, -1);
				response.setStatusCode((byte) 2);
			}
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to submit task request to master " + p_request + " failed.");
		}
	}

	/**
	 * Handle an incoming GetMasterStatusRequest
	 *
	 * @param p_request GetMasterStatusRequest
	 */
	private void incomingGetMasterStatusRequest(final GetMasterStatusRequest p_request) {
		GetMasterStatusResponse response;

		StatusMaster status = getStatusMaster();
		response = new GetMasterStatusResponse(p_request, status);

		// check first if we are a master
		if (status == null) {
			response.setStatusCode((byte) 1);
		} else {
			response.setStatusCode((byte) 0);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to master status request " + p_request + " failed.");
		}
	}

	/**
	 * Handle an incoming TaskExecutionStartedMessage
	 *
	 * @param p_message TaskExecutionStartedMessage
	 */
	private void incomingTaskExecutionStartedMessage(final TaskExecutionStartedMessage p_message) {

		// that's a little risky, but setting the id in the submit call
		// is done quickly enough that we can keep blocking a little here
		// though if we get weird timeouts or network message behavior
		// this section might be the cause
		Task task = m_remoteTasks.get(p_message.getTaskPayloadId());
		while (task == null) {
			task = m_remoteTasks.get(p_message.getTaskPayloadId());
			Thread.yield();
		}

		task.getPayload().setPayloadId(p_message.getTaskPayloadId());
		task.getPayload().setSalves(p_message.getSlavesAssignedForExecution());

		task.notifyListenersExecutionStarts();
	}

	/**
	 * Handle an incoming TaskExecutionFinishedMessage
	 *
	 * @param p_message TaskExecutionFinishedMessage
	 */
	private void incomingTaskExecutionFinishedMessage(final TaskExecutionFinishedMessage p_message) {
		// that's a little risky, but setting the id in the submit call
		// is done quickly enough that we can keep blocking a little here
		// though if we get weird timeouts or network message behavior
		// this section might be the cause
		Task task = m_remoteTasks.remove(p_message.getTaskPayloadId());
		while (task == null) {
			task = m_remoteTasks.remove(p_message.getTaskPayloadId());
			Thread.yield();
		}

		// done with task, remove
		// get return codes of execution
		task.getPayload().setExecutionReturnCodes(p_message.getExecutionReturnCodes());
		task.notifyListenersExecutionCompleted();
	}

	/**
	 * Register various (built in) task payloads
	 */
	private void registerTaskPayloads() {
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_NULL_TASK,
				NullTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_SLAVE_PRINT_INFO_TASK,
				SlavePrintInfoTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_WAIT_TASK,
				WaitTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_PRINT_TASK,
				PrintTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_CONSOLE_TASK,
				PrintMemoryStatusToConsoleTask.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_FILE_TASK,
				PrintMemoryStatusToFileTask.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_CONSOLE_TASK,
				PrintStatisticsToConsoleTask.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_FILE_TASK,
				PrintStatisticsToFileTask.class);
	}

	/**
	 * Status object of a master compute node.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 12.02.16
	 */
	public static class StatusMaster implements Importable, Exportable {
		private short m_masterNodeId;
		private AbstractComputeMSBase.State m_state;
		private int m_numTasksQueued;
		private int m_tasksProcessed;
		private ArrayList<Short> m_connectedSlaves;

		/**
		 * Constructor
		 */
		public StatusMaster() {
			m_masterNodeId = NodeID.INVALID_ID;
			m_state = AbstractComputeMSBase.State.STATE_INVALID;
			m_connectedSlaves = new ArrayList<Short>();
			m_numTasksQueued = 0;
		}

		/**
		 * Constructor
		 *
		 * @param p_masterNodeId    Node id of the master
		 * @param p_state           The current state of the instance
		 * @param p_connectedSlaves List of connected slave ids to this master.
		 * @param p_numTasksQueued  Number of tasks queued currently on this master.
		 * @param p_tasksProcessed  Number of tasks processed so far.
		 */
		public StatusMaster(final short p_masterNodeId, final AbstractComputeMSBase.State p_state,
				final ArrayList<Short> p_connectedSlaves,
				final int p_numTasksQueued,
				final int p_tasksProcessed) {
			m_masterNodeId = p_masterNodeId;
			m_state = p_state;
			m_connectedSlaves = p_connectedSlaves;
			m_numTasksQueued = p_numTasksQueued;
			m_tasksProcessed = p_tasksProcessed;
		}

		/**
		 * Get the node if of the master of the group.
		 *
		 * @return Node if of the master
		 */
		public short getMasterNodeId() {
			return m_masterNodeId;
		}

		/**
		 * Get the current state of the instance.
		 *
		 * @return Current state
		 */
		public AbstractComputeMSBase.State getState() {
			return m_state;
		}

		/**
		 * Get the list of slaves that are connected to the master.
		 *
		 * @return List of slave ids.
		 */
		public ArrayList<Short> getConnectedSlaves() {
			return m_connectedSlaves;
		}

		/**
		 * Get the number of currently queued tasks.
		 *
		 * @return Number of queued tasks.
		 */
		public int getNumTasksQueued() {
			return m_numTasksQueued;
		}

		/**
		 * Get the number of tasks processed so far.
		 *
		 * @return Number of tasks processed.
		 */
		public int getNumTasksProcessed() {
			return m_numTasksQueued;
		}

		@Override
		public int exportObject(final Exporter p_exporter, final int p_size) {
			p_exporter.writeShort(m_masterNodeId);
			p_exporter.writeInt(m_state.ordinal());
			p_exporter.writeInt(m_connectedSlaves.size());
			for (short slave : m_connectedSlaves) {
				p_exporter.writeShort(slave);
			}
			p_exporter.writeInt(m_numTasksQueued);
			p_exporter.writeInt(m_tasksProcessed);

			return sizeofObject();
		}

		@Override
		public int importObject(final Importer p_importer, final int p_size) {
			m_masterNodeId = p_importer.readShort();
			m_state = AbstractComputeMSBase.State.values()[p_importer.readInt()];
			int size = p_importer.readInt();
			m_connectedSlaves = new ArrayList<Short>(size);
			for (int i = 0; i < size; i++) {
				m_connectedSlaves.add(p_importer.readShort());
			}
			m_numTasksQueued = p_importer.readInt();
			m_tasksProcessed = p_importer.readInt();

			return sizeofObject();
		}

		@Override
		public int sizeofObject() {
			return Short.BYTES + Integer.BYTES + Integer.BYTES + m_connectedSlaves.size() * Short.BYTES + Integer.BYTES
					+ Integer.BYTES;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return true;
		}

		@Override
		public String toString() {
			String str = new String();
			str += "Master: " + NodeID.toHexString(m_masterNodeId) + "\n";
			str += "State: " + m_state + "\n";
			str += "Tasks queued: " + m_numTasksQueued + "\n";
			str += "Tasks processed: " + m_tasksProcessed + "\n";
			str += "Connected slaves(" + m_connectedSlaves.size() + "):\n";
			for (int i = 0; i < m_connectedSlaves.size(); i++) {
				str += i + ": " + NodeID.toHexString(m_connectedSlaves.get(i));
				if (m_connectedSlaves.size() > 0 && i < m_connectedSlaves.size() - 1) {
					str += "\n";
				}
			}
			return str;
		}
	}
}
