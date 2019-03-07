/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.ms;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.ms.messages.GetMasterStatusRequest;
import de.hhu.bsinfo.dxram.ms.messages.GetMasterStatusResponse;
import de.hhu.bsinfo.dxram.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxram.ms.messages.SubmitTaskRequest;
import de.hhu.bsinfo.dxram.ms.messages.SubmitTaskResponse;
import de.hhu.bsinfo.dxram.ms.messages.TaskExecutionFinishedMessage;
import de.hhu.bsinfo.dxram.ms.messages.TaskExecutionStartedMessage;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptGsonContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * DXRAM service providing a master slave based distributed task execution framework for computation on DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class MasterSlaveComputeService extends AbstractDXRAMService<MasterSlaveComputeServiceConfig>
        implements MessageReceiver, TaskListener {
    // component dependencies
    private NetworkComponent m_network;
    private NameserviceComponent m_nameservice;
    private AbstractBootComponent m_boot;
    private LookupComponent m_lookup;
    private PluginComponent m_plugin;

    private AbstractComputeMSBase m_computeMSInstance;

    private ConcurrentMap<Integer, TaskScriptState> m_remoteTasks = new ConcurrentHashMap<>();
    private AtomicInteger m_taskIdCounter = new AtomicInteger(0);

    /**
     * Create an instance of a task denoted by its task (command) name
     *
     * @param p_taskName
     *         Name (command) of the task
     * @param p_args
     *         Arguments to provide to the task object
     * @return A task instance
     */
    public Task createTaskInstance(final String p_taskName, final Object... p_args) {
        Class<?> clazz;

        try {
            clazz = m_plugin.getClassByName(p_taskName);
        } catch (final ClassNotFoundException ignored) {
            LOGGER.error("Cannot find task class: %s", p_taskName);
            return null;
        }

        // check if class implements Task interface
        boolean impl = false;

        for (Class<?> iface : clazz.getInterfaces()) {
            if (iface.equals(Task.class)) {
                impl = true;
                break;
            }
        }

        if (!impl) {
            LOGGER.error("Class '%s' does not implement the Task interface");
            return null;
        }

        for (Constructor constructor : clazz.getConstructors()) {

            try {
                return (Task) constructor.newInstance(p_args);
            } catch (final SecurityException | InstantiationException | IllegalAccessException |
                    IllegalArgumentException | InvocationTargetException e) {
            }
        }

        LOGGER.error("Cannot create instance of Task '%s'", p_taskName);
        return null;
    }

    /**
     * Read a task script from a json formatted file
     *
     * @param p_taskScriptFileName
     *         File to read
     * @return Read task script or null on read failure
     */
    public TaskScript readTaskScriptFromJsonFile(final String p_taskScriptFileName) {
        Gson gson = TaskScriptGsonContext.createGsonInstance(m_plugin);

        try {
            return gson.fromJson(new String(Files.readAllBytes(Paths.get(p_taskScriptFileName))), TaskScript.class);
        } catch (final IOException ignored) {
            return null;
        }
    }

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
    public ArrayList<MasterNodeEntry> getMasters() {
        ArrayList<MasterNodeEntry> masters = new ArrayList<>();

        // check the name service entries
        for (int i = 0; i <= AbstractComputeMSBase.MAX_COMPUTE_GROUP_ID; i++) {
            long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + i, 0);
            if (tmp != -1) {
                masters.add(new MasterNodeEntry(ChunkID.getCreatorID(tmp), (byte) i));
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

            LOGGER.error("Cannot get status on non master node type");

            return null;
        }

        ArrayList<Short> slaves = ((ComputeMaster) m_computeMSInstance).getConnectedSlaves();
        int numTasksInQueue = ((ComputeMaster) m_computeMSInstance).getNumberOfTasksInQueue();
        AbstractComputeMSBase.State state = m_computeMSInstance.getComputeState();
        int tasksProcessed = ((ComputeMaster) m_computeMSInstance).getTotalTaskScriptsProcessed();

        return new StatusMaster(m_boot.getNodeId(), state, slaves, numTasksInQueue, tasksProcessed);
    }

    /**
     * Get the status of a remote master node.
     *
     * @param p_computeGroupId
     *         Compute group id to get the master's status of
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
        long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + p_computeGroupId, 0);

        if (tmp == -1) {
            LOGGER.error("Cannot find master node of compute gropu id %d", p_computeGroupId);
            return null;
        }

        masterNodeId = ChunkID.getCreatorID(tmp);

        GetMasterStatusRequest request = new GetMasterStatusRequest(masterNodeId);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Getting status of master 0x%X failed: %s", masterNodeId, e);

            return null;
        }

        GetMasterStatusResponse response = (GetMasterStatusResponse) request.getResponse();

        if (response.getStatus() != 0) {

            LOGGER.error("Cannot get status on non master node 0x%X", masterNodeId);

            return null;
        }

        return response.getStatusMaster();
    }

    /**
     * Submit a task script to this master.
     *
     * @param p_taskScript
     *         TaskScript to submit to this master.
     * @param p_listener
     *         Listener to register to register with the generated task state
     * @return TaskScriptState containing assigned task script id and registered listeners or null on error
     */
    public TaskScriptState submitTaskScript(final TaskScript p_taskScript, final TaskListener... p_listener) {
        if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {

            LOGGER.error("Cannot submit task %s on non master node type", p_taskScript);

            return null;
        }

        TaskScriptState state = new TaskScriptState(p_taskScript);
        state.assignTaskId(m_taskIdCounter.getAndIncrement());
        state.registerTaskListener(p_listener);

        if (((ComputeMaster) m_computeMSInstance).submitTask(state)) {
            return state;
        } else {
            return null;
        }
    }

    /**
     * Submit a task script to remote master node.
     *
     * @param p_taskScript
     *         TaskScript to submit to another master node.
     * @param p_computeGroupId
     *         Compute group to submit the task script to.
     * @param p_listener
     *         Listener to register to register with the generated task state
     * @return TaskScriptState containing assigned task script id and registered listeners or null on error
     */
    public TaskScriptState submitTaskScript(final TaskScript p_taskScript, final short p_computeGroupId,
            final TaskListener... p_listener) {
        if (m_computeMSInstance.getRole() == ComputeRole.MASTER) {
            ComputeMaster master = (ComputeMaster) m_computeMSInstance;
            if (master.getComputeGroupId() == p_computeGroupId) {
                return submitTaskScript(p_taskScript, p_listener);
            }
        }

        // get the node id of the master node of the group
        short masterNodeId;
        {
            long tmp = m_nameservice.getChunkID(AbstractComputeMSBase.NAMESERVICE_ENTRY_IDENT + p_computeGroupId, 0);
            if (tmp == -1) {

                LOGGER.error("Cannot find master node of compute group id %d", p_computeGroupId);

                return null;
            }
            masterNodeId = ChunkID.getCreatorID(tmp);
        }

        SubmitTaskRequest request = new SubmitTaskRequest(masterNodeId, p_taskScript);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Sending submit task script request to node 0x%X failed: %s", masterNodeId, e);

            return null;
        }

        SubmitTaskResponse response = (SubmitTaskResponse) request.getResponse();
        if (response.getStatus() != 0) {

            LOGGER.error("Error submitting task script, code %d", response.getStatus());

            return null;
        }

        // remember task for remote callbacks
        TaskScriptState state = new TaskScriptState(p_taskScript);
        state.assignTaskId(response.getAssignedPayloadId());
        state.setNodeIdSubmitted(m_boot.getNodeId());
        state.registerTaskListener(p_listener);
        m_remoteTasks.put(state.getTaskScriptIdAssigned(), state);

        LOGGER.info("Submitted task to compute group %d with master node 0x%X: %s", p_computeGroupId, masterNodeId,
                p_taskScript);

        return state;
    }

    @Override
    public void taskBeforeExecution(final TaskScriptState p_taskScriptState) {
        // only used for remote tasks to callback the node they were submitted on
        TaskExecutionStartedMessage message =
                new TaskExecutionStartedMessage(p_taskScriptState.getNodeIdSubmitted(),
                        p_taskScriptState.getTaskScriptIdAssigned());

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {

            LOGGER.error("Sending remote callback before execution to node %d failed",
                    p_taskScriptState.getNodeIdSubmitted());

        }
    }

    @Override
    public void taskCompleted(final TaskScriptState p_taskScriptState) {
        // only used for remote tasks to callback the node they were submitted on
        TaskExecutionFinishedMessage message =
                new TaskExecutionFinishedMessage(p_taskScriptState.getNodeIdSubmitted(),
                        p_taskScriptState.getTaskScriptIdAssigned(),
                        p_taskScriptState.getExecutionReturnCodes());

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {

            LOGGER.error("Sending remote callback completed to node 0x%X failed",
                    p_taskScriptState.getNodeIdSubmitted());

        }

        // we don't have to remember this remote task anymore
        m_remoteTasks.remove(p_taskScriptState.getTaskScriptIdAssigned());
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE) {
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
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_plugin = p_componentAccessor.getComponent(PluginComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST, SubmitTaskRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE, SubmitTaskResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST,
                GetMasterStatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_RESPONSE,
                GetMasterStatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE,
                TaskExecutionStartedMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE,
                TaskExecutionFinishedMessage.class);

        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST, this);
        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE, this);

        switch (ComputeRole.toComputeRole(getConfig().getRole())) {
            case MASTER:
                m_computeMSInstance = new ComputeMaster(getConfig().getComputeGroupId(),
                        getConfig().getPingInterval().getMs(), getParentEngine(), m_network, m_nameservice, m_boot,
                        m_lookup, m_plugin);
                break;

            case SLAVE:
                m_computeMSInstance =
                        new ComputeSlave(getConfig().getComputeGroupId(), getConfig().getPingInterval().getMs(),
                                getParentEngine(), m_network, m_nameservice, m_boot, m_lookup, m_plugin);
                break;

            case NONE:
                m_computeMSInstance = new ComputeNone(getParentEngine(), m_network, m_nameservice, m_boot, m_lookup,
                        m_plugin);
                break;

            default:
                assert false;
                break;
        }

        LOGGER.debug("Started compute node type '%s' with compute group id %d", getConfig().getRole(),
                getConfig().getComputeGroupId());

        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_computeMSInstance.shutdown();
        m_computeMSInstance = null;

        return true;
    }

    /**
     * Handle an incoming SubmitTaskRequest
     *
     * @param p_request
     *         SubmitTaskRequest
     */
    private void incomingSubmitTaskRequest(final SubmitTaskRequest p_request) {
        SubmitTaskResponse response;

        LOGGER.debug("Incoming remote submit task script request %s", p_request);

        if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
            LOGGER.error("Cannot submit remote task script %s on non master node type", p_request.getTaskScript());

            response = new SubmitTaskResponse(p_request, (short) -1, -1, (byte) 1);
        } else {
            // complete reflection of generic task objects
            m_computeMSInstance.reflectIncomingNodeDataInstances(p_request.getTaskScript());

            TaskScriptState taskScriptState = new TaskScriptState(p_request.getTaskScript());
            taskScriptState.assignTaskId(m_taskIdCounter.getAndIncrement());
            taskScriptState.setNodeIdSubmitted(p_request.getSource());
            taskScriptState.registerTaskListener(this);

            boolean ret = ((ComputeMaster) m_computeMSInstance).submitTask(taskScriptState);

            if (ret) {
                m_remoteTasks.put(taskScriptState.getTaskScriptIdAssigned(), taskScriptState);

                response = new SubmitTaskResponse(p_request, m_computeMSInstance.getComputeGroupId(),
                        taskScriptState.getTaskScriptIdAssigned(), (byte) 0);
            } else {
                response = new SubmitTaskResponse(p_request, (short) -1, -1, (byte) 2);
            }
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            LOGGER.error("Sending response to submit task request to master %s failed", p_request);
        }
    }

    /**
     * Handle an incoming GetMasterStatusRequest
     *
     * @param p_request
     *         GetMasterStatusRequest
     */
    private void incomingGetMasterStatusRequest(final GetMasterStatusRequest p_request) {
        GetMasterStatusResponse response;

        StatusMaster status = getStatusMaster();
        // check first if we are a master
        if (status == null) {
            response = new GetMasterStatusResponse(p_request, null, (byte) 1);
        } else {
            response = new GetMasterStatusResponse(p_request, status, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to master status request %s failed", p_request);

        }
    }

    /**
     * Handle an incoming TaskExecutionStartedMessage
     *
     * @param p_message
     *         TaskExecutionStartedMessage
     */
    private void incomingTaskExecutionStartedMessage(final TaskExecutionStartedMessage p_message) {

        // that's a little risky, but setting the id in the submit call
        // is done quickly enough that we can keep blocking here
        // though if we get weird timeouts or network message behavior
        // this section might be the cause
        TaskScriptState taskScriptState = m_remoteTasks.get(p_message.getTaskPayloadId());
        while (taskScriptState == null) {
            taskScriptState = m_remoteTasks.get(p_message.getTaskPayloadId());
            Thread.yield();
        }

        taskScriptState.notifyListenersExecutionStarts();
    }

    /**
     * Handle an incoming TaskExecutionFinishedMessage
     *
     * @param p_message
     *         TaskExecutionFinishedMessage
     */
    private void incomingTaskExecutionFinishedMessage(final TaskExecutionFinishedMessage p_message) {
        // that's a little risky, but setting the id in the submit call
        // is done quickly enough that we can keep blocking a little here
        // though if we get weird timeouts or network message behavior
        // this section might be the cause
        TaskScriptState taskScriptState = m_remoteTasks.remove(p_message.getTaskPayloadId());
        while (taskScriptState == null) {
            taskScriptState = m_remoteTasks.remove(p_message.getTaskPayloadId());
            Thread.yield();
        }

        // done with taskScript, remove
        // get return codes of execution
        taskScriptState.notifyListenersExecutionCompleted(p_message.getExecutionReturnCodes());
    }

    /**
     * Status object of a master compute node.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.02.2016
     */
    public static class StatusMaster implements Importable, Exportable {
        private short m_masterNodeId;
        private AbstractComputeMSBase.State m_state;
        private int m_numTaskScriptsQueued;
        private int m_taskScriptsProcessed;
        private ArrayList<Short> m_connectedSlaves;

        private int m_connectedSlavesToRead; // For serialization, only

        /**
         * Constructor
         */
        public StatusMaster() {
            m_masterNodeId = NodeID.INVALID_ID;
            m_state = AbstractComputeMSBase.State.STATE_INVALID;
            m_numTaskScriptsQueued = 0;
        }

        /**
         * Constructor
         *
         * @param p_masterNodeId
         *         Node id of the master
         * @param p_state
         *         The current state of the instance
         * @param p_connectedSlaves
         *         List of connected slave ids to this master.
         * @param p_numTaskScriptsQueued
         *         Number of task scripts queued currently on this master.
         * @param p_taskScriptsProcessed
         *         Number of task scripts processed so far.
         */
        StatusMaster(final short p_masterNodeId, final AbstractComputeMSBase.State p_state,
                final ArrayList<Short> p_connectedSlaves,
                final int p_numTaskScriptsQueued, final int p_taskScriptsProcessed) {
            m_masterNodeId = p_masterNodeId;
            m_state = p_state;
            m_connectedSlaves = p_connectedSlaves;
            m_numTaskScriptsQueued = p_numTaskScriptsQueued;
            m_taskScriptsProcessed = p_taskScriptsProcessed;
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
            return m_numTaskScriptsQueued;
        }

        /**
         * Get the number of tasks processed so far.
         *
         * @return Number of tasks processed.
         */
        public int getNumTaskScriptsProcessed() {
            return m_taskScriptsProcessed;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeShort(m_masterNodeId);
            p_exporter.writeInt(m_state.ordinal());
            p_exporter.writeInt(m_connectedSlaves.size());
            m_connectedSlaves.forEach(p_exporter::writeShort);
            p_exporter.writeInt(m_numTaskScriptsQueued);
            p_exporter.writeInt(m_taskScriptsProcessed);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_masterNodeId = p_importer.readShort(m_masterNodeId);

            int tmp = p_importer.readInt(0);
            if (m_state == AbstractComputeMSBase.State.STATE_INVALID) {
                m_state = AbstractComputeMSBase.State.values()[tmp];
            }

            m_connectedSlavesToRead = p_importer.readInt(m_connectedSlavesToRead);
            if (m_connectedSlaves == null) {
                m_connectedSlaves = new ArrayList<>(m_connectedSlavesToRead);
            }
            for (int i = 0; i < m_connectedSlavesToRead; i++) {
                short slave = p_importer.readShort((short) 0);
                if (m_connectedSlaves.size() == i) {
                    m_connectedSlaves.add(slave);
                }
            }
            m_numTaskScriptsQueued = p_importer.readInt(m_numTaskScriptsQueued);
            m_taskScriptsProcessed = p_importer.readInt(m_taskScriptsProcessed);
        }

        @Override
        public int sizeofObject() {
            return Short.BYTES + Integer.BYTES + Integer.BYTES + m_connectedSlaves.size() * Short.BYTES +
                    Integer.BYTES + Integer.BYTES;
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();

            str.append("Master: ").append(NodeID.toHexString(m_masterNodeId)).append('\n');
            str.append("State: ").append(m_state).append('\n');
            str.append("Task scripts queued: ").append(m_numTaskScriptsQueued).append('\n');
            str.append("Task scripts processed: ").append(m_taskScriptsProcessed).append('\n');
            str.append("Connected slaves(").append(m_connectedSlaves.size()).append("):\n");

            for (int i = 0; i < m_connectedSlaves.size(); i++) {
                str.append(i).append(": ").append(NodeID.toHexString(m_connectedSlaves.get(i)));

                if (!m_connectedSlaves.isEmpty() && i < m_connectedSlaves.size() - 1) {
                    str.append('\n');
                }
            }

            return str.toString();
        }
    }
}
