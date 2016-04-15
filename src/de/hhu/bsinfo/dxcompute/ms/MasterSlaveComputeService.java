
package de.hhu.bsinfo.dxcompute.ms;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import de.hhu.bsinfo.dxcompute.ms.messages.GetMasterStatusRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.GetMasterStatusResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SubmitTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SubmitTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.TaskRemoteCallbackMessage;
import de.hhu.bsinfo.dxcompute.ms.tcmd.TcmdMSMasterList;
import de.hhu.bsinfo.dxcompute.ms.tcmd.TcmdMSMasterStatus;
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

public class MasterSlaveComputeService extends AbstractDXRAMService implements MessageReceiver, TaskListener {

	private NetworkComponent m_network;
	private NameserviceComponent m_nameservice;
	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;
	private TerminalComponent m_terminal;

	private ComputeMSBase m_computeMSInstance;

	private ConcurrentMap<Long, Task> m_remoteTasks = new ConcurrentHashMap<Long, Task>();

	public ComputeRole getComputeRole() {
		return m_computeMSInstance.getRole();
	}

	/**
	 * Get a list of all available master nodes.
	 * @return List of available master nodes with their compute group id
	 */
	public ArrayList<Pair<Short, Byte>> getMasters() {
		ArrayList<Pair<Short, Byte>> masters = new ArrayList<Pair<Short, Byte>>();

		// check the name service entries
		for (int i = 0; i <= ComputeMSBase.MAX_COMPUTE_GROUP_ID; i++) {
			long tmp = m_nameservice.getChunkID(ComputeMSBase.NAMESERVICE_ENTRY_IDENT + i, 0);
			if (tmp != -1) {
				masters.add(new Pair<Short, Byte>(ChunkID.getCreatorID(tmp), (byte) i));
			}
		}

		return masters;
	}

	public StatusMaster getStatusMaster() {
		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(), "Cannot get status on non master node type");
			return null;
		}

		ArrayList<Short> slaves = ((ComputeMaster) m_computeMSInstance).getConnectedSlaves();
		int numTasksInQueue = ((ComputeMaster) m_computeMSInstance).getNumberOfTasksInQueue();

		return new StatusMaster(slaves, numTasksInQueue);
	}

	public StatusMaster getStatusMaster(final short p_masterNodeId) {
		if (p_masterNodeId == m_boot.getNodeID()) {
			return getStatusMaster();
		}

		GetMasterStatusRequest request = new GetMasterStatusRequest(p_masterNodeId);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Getting status of master " + NodeID.toHexString(p_masterNodeId) + " failed: " + err);
			return null;
		}

		GetMasterStatusResponse response = (GetMasterStatusResponse) request.getResponse();
		if (response.getStatusCode() != 0) {
			m_logger.error(getClass(), "Cannot get status on non master node " + NodeID.toHexString(p_masterNodeId));
			return null;
		}

		return response.getStatusMaster();
	}

	public boolean submitTask(final Task p_task) {
		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(), "Cannot submit task " + p_task + " on non master node type");
			return false;
		}

		p_task.setNodeIdSubmitted(m_boot.getNodeID());
		p_task.m_serviceAccessor = getServiceAccessor();
		return ((ComputeMaster) m_computeMSInstance).submitTask(p_task);
	}

	// note remotely submitted tasks do not support
	public boolean submitTask(final Task p_task, final short p_masterNodeId) {
		if (p_masterNodeId == m_boot.getNodeID()) {
			return submitTask(p_task);
		}

		p_task.setNodeIdSubmitted(m_boot.getNodeID());

		SubmitTaskRequest request = new SubmitTaskRequest(p_masterNodeId, p_task.getPayload());
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending submit task request to node " + NodeID.toHexString(p_masterNodeId) + " failed: " + err);
			return false;
		}

		SubmitTaskResponse response = (SubmitTaskResponse) request.getResponse();
		if (response.getStatusCode() != 0) {
			m_logger.error(getClass(), "Error submitting task, code " + response.getStatusCode());
			return false;
		}

		// get assigned payload id
		p_task.getPayload().setPayloadId(response.getAssignedPayloadId());

		// remember task for remote callbacks
		m_remoteTasks.put(p_task.getPayload().getPayloadId(), p_task);

		return true;
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		// only used for remote tasks to callback the node they were submitted on
		TaskRemoteCallbackMessage message =
				new TaskRemoteCallbackMessage(p_task.getNodeIdSubmitted(), p_task.getPayload().getPayloadId(), 0);

		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending remote callback before execution to node " + p_task.getNodeIdSubmitted() + " failed.");
		}
	}

	@Override
	public void taskCompleted(final Task p_task) {
		// only used for remote tasks to callback the node they were submitted on
		TaskRemoteCallbackMessage message =
				new TaskRemoteCallbackMessage(p_task.getNodeIdSubmitted(), p_task.getPayload().getPayloadId(), 0);

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
					case MasterSlaveMessages.SUBTYPE_TASK_REMOTE_CALLBACK_MESSAGE:
						incomingTaskRemoteCallbackMessage((TaskRemoteCallbackMessage) p_message);
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
	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		ComputeRole role =
				ComputeRole.toComputeRole(p_settings.getValue(MasterSlaveConfigurationValues.Service.ROLE));
		int computeGroupId = p_settings.getValue(MasterSlaveConfigurationValues.Service.COMPUTE_GROUP_ID);

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
				MasterSlaveMessages.SUBTYPE_TASK_REMOTE_CALLBACK_MESSAGE,
				TaskRemoteCallbackMessage.class);

		m_network.register(SubmitTaskRequest.class, this);
		m_network.register(GetMasterStatusRequest.class, this);
		m_network.register(TaskRemoteCallbackMessage.class, this);

		m_terminal.registerCommand(new TcmdMSMasterList());
		m_terminal.registerCommand(new TcmdMSMasterStatus());

		switch (role) {
			case MASTER:
				m_computeMSInstance = new ComputeMaster(computeGroupId, getServiceAccessor(), m_network, m_logger,
						m_nameservice, m_boot);
				break;
			case SLAVE:
				m_computeMSInstance = new ComputeSlave(computeGroupId, getServiceAccessor(), m_network, m_logger,
						m_nameservice, m_boot);
				break;
			case NONE:
				m_computeMSInstance = new ComputeNone(computeGroupId, getServiceAccessor(), m_network, m_logger,
						m_nameservice, m_boot);
				break;
			default:
				assert 1 == 2;
				break;
		}

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

	private void incomingSubmitTaskRequest(final SubmitTaskRequest p_request) {
		SubmitTaskResponse response;

		m_logger.debug(getClass(), "Incoming remote submit task request " + p_request);

		if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
			m_logger.error(getClass(), "Cannot submit remote task " + p_request.getTask() + " on non master node type");

			response = new SubmitTaskResponse(p_request, -1);
			response.setStatusCode((byte) 1);
		} else {
			Task task = new Task(p_request.getTask(), "RemoteTask" + p_request);
			task.setNodeIdSubmitted(p_request.getSource());
			task.m_serviceAccessor = getServiceAccessor();
			task.registerTaskListener(this);

			boolean ret = ((ComputeMaster) m_computeMSInstance).submitTask(task);

			if (ret) {
				m_remoteTasks.put(task.getPayload().getPayloadId(), task);

				response = new SubmitTaskResponse(p_request, task.getPayload().getPayloadId());
				response.setStatusCode((byte) 0);
			} else {
				response = new SubmitTaskResponse(p_request, -1);
				response.setStatusCode((byte) 2);
			}
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to submit task request to master " + p_request + " failed.");
		}
	}

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

	private void incomingTaskRemoteCallbackMessage(final TaskRemoteCallbackMessage p_request) {

		switch (p_request.getCallbackId()) {
			case 0: {
				Task task = m_remoteTasks.get(p_request.getTaskPayloadId());
				task.notifyListenersExecutionStarts();
				break;
			}
			case 1: {
				// done with task, remove
				Task task = m_remoteTasks.remove(p_request.getTaskPayloadId());
				task.notifyListenersExecutionCompleted();
				break;
			}
			default:
				assert 1 == 2;
				break;
		}

	}

	public static class StatusMaster implements Importable, Exportable {
		private ArrayList<Short> m_connectedSlaves;
		private int m_numTasksQueued;

		public StatusMaster() {
			m_connectedSlaves = new ArrayList<Short>();
			m_numTasksQueued = 0;
		}

		public StatusMaster(final ArrayList<Short> p_connectedSlaves, final int p_numTasksQueued) {
			m_connectedSlaves = p_connectedSlaves;
			m_numTasksQueued = p_numTasksQueued;
		}

		public ArrayList<Short> getConnectedSlaves() {
			return m_connectedSlaves;
		}

		public int getNumTasksQueued() {
			return m_numTasksQueued;
		}

		@Override
		public int exportObject(final Exporter p_exporter, final int p_size) {
			p_exporter.writeInt(m_connectedSlaves.size());
			for (short slave : m_connectedSlaves) {
				p_exporter.writeShort(slave);
			}
			p_exporter.writeInt(m_numTasksQueued);

			return sizeofObject();
		}

		@Override
		public int importObject(final Importer p_importer, final int p_size) {
			int size = p_importer.readInt();
			m_connectedSlaves = new ArrayList<Short>(size);
			for (int i = 0; i < size; i++) {
				m_connectedSlaves.add(p_importer.readShort());
			}
			m_numTasksQueued = p_importer.readInt();

			return sizeofObject();
		}

		@Override
		public int sizeofObject() {
			return Integer.BYTES + m_connectedSlaves.size() * Short.BYTES + Integer.BYTES;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return true;
		}

		@Override
		public String toString() {
			String str = new String();
			str += "Connected slaves(" + m_connectedSlaves.size() + "):\n";
			for (short slave : m_connectedSlaves) {
				str += NodeID.toHexString(slave) + "\n";
			}
			str += "Tasks queued: " + m_numTasksQueued;
			return str;
		}
	}
}
