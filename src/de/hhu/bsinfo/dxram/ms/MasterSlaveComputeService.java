/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.ms;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.ms.messages.GetMasterStatusRequest;
import de.hhu.bsinfo.dxram.ms.messages.GetMasterStatusResponse;
import de.hhu.bsinfo.dxram.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxram.ms.messages.SubmitTaskRequest;
import de.hhu.bsinfo.dxram.ms.messages.SubmitTaskResponse;
import de.hhu.bsinfo.dxram.ms.messages.TaskExecutionFinishedMessage;
import de.hhu.bsinfo.dxram.ms.messages.TaskExecutionStartedMessage;
import de.hhu.bsinfo.dxram.ms.tcmd.TcmdCompgrpls;
import de.hhu.bsinfo.dxram.ms.tcmd.TcmdCompgrpstatus;
import de.hhu.bsinfo.dxram.ms.tcmd.TcmdComptask;
import de.hhu.bsinfo.dxram.ms.tcmd.TcmdComptaskscript;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * DXRAM service providing a master slave based distributed task execution framework for computation on DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class MasterSlaveComputeService extends AbstractDXRAMService implements MessageReceiver, TaskListener {

    private static final Logger LOGGER = LogManager.getFormatterLogger(MasterSlaveComputeService.class.getSimpleName());

    // configuration values
    /**
     * Compute role to assign to the current instance (master, slave or none)
     */
    @Expose
    private String m_role = ComputeRole.NONE.toString();
    /**
     * Compute group id for the current instance (ignored on none)
     */
    @Expose
    private short m_computeGroupId = 0;
    /**
     * Keep alive ping time for master to contact slaves
     */
    @Expose
    private TimeUnit m_pingInterval = new TimeUnit(1, TimeUnit.SEC);

    // dependent components
    private NetworkComponent m_network;
    private NameserviceComponent m_nameservice;
    private AbstractBootComponent m_boot;
    private LookupComponent m_lookup;
    private TerminalComponent m_terminal;

    private AbstractComputeMSBase m_computeMSInstance;

    private ConcurrentMap<Integer, TaskScriptState> m_remoteTasks = new ConcurrentHashMap<>();
    private AtomicInteger m_taskIdCounter = new AtomicInteger(0);

    /**
     * Constructor
     */
    public MasterSlaveComputeService() {
        super("mscomp");
    }

    /**
     * Create an instance of a task denoted by its task (command) name
     *
     * @param p_taskName
     *     Name (command) of the task
     * @param p_args
     *     Arguments to provide to the task object
     * @return A task instance
     */
    public static Task createTaskInstance(final String p_taskName, final Object... p_args) {
        Class<?> clazz;
        try {
            clazz = Class.forName(p_taskName);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Cannot find task class " + p_taskName);
        }

        try {
            return (Task) clazz.getConstructor().newInstance(p_args);
        } catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException("Cannot create instance of Task " + p_taskName);
        }
    }

    /**
     * Read a task script from a json formatted file
     *
     * @param p_taskScriptFileName
     *     File to read
     * @return Read task script or null on read failure
     */
    public static TaskScript readTaskScriptFromJsonFile(final String p_taskScriptFileName) {
        Gson gson = TaskScriptGsonContext.createGsonInstance();

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
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot get status on non master node type");
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        ArrayList<Short> slaves = ((ComputeMaster) m_computeMSInstance).getConnectedSlaves();
        int numTasksInQueue = ((ComputeMaster) m_computeMSInstance).getNumberOfTasksInQueue();
        AbstractComputeMSBase.State state = m_computeMSInstance.getComputeState();
        int tasksProcessed = ((ComputeMaster) m_computeMSInstance).getTotalTaskScriptsProcessed();

        return new StatusMaster(m_boot.getNodeID(), state, slaves, numTasksInQueue, tasksProcessed);
    }

    /**
     * Get the status of a remote master node.
     *
     * @param p_computeGroupId
     *     Compute group id to get the master's status of
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
                // #if LOGGER >= ERROR
                LOGGER.error("Cannot find master node of compute gropu id %d", p_computeGroupId);
                // #endif /* LOGGER >= ERROR */
                return null;
            }
            masterNodeId = ChunkID.getCreatorID(tmp);
        }

        GetMasterStatusRequest request = new GetMasterStatusRequest(masterNodeId);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting status of master 0x%X failed: %s", masterNodeId, e);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        GetMasterStatusResponse response = (GetMasterStatusResponse) request.getResponse();
        if (response.getStatusCode() != 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot get status on non master node 0x%X", masterNodeId);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        return response.getStatusMaster();
    }

    /**
     * Submit a task script to this master.
     *
     * @param p_taskScript
     *     TaskScript to submit to this master.
     * @param p_listener
     *     Listener to register to register with the generated task state
     * @return TaskScriptState containing assigned task script id and registered listeners or null on error
     */
    public TaskScriptState submitTaskScript(final TaskScript p_taskScript, final TaskListener... p_listener) {
        if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot submit task %s on non master node type", p_taskScript);
            // #endif /* LOGGER >= ERROR */
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
     *     TaskScript to submit to another master node.
     * @param p_computeGroupId
     *     Compute group to submit the task script to.
     * @param p_listener
     *     Listener to register to register with the generated task state
     * @return TaskScriptState containing assigned task script id and registered listeners or null on error
     */
    public TaskScriptState submitTaskScript(final TaskScript p_taskScript, final short p_computeGroupId, final TaskListener... p_listener) {
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
                // #if LOGGER >= ERROR
                LOGGER.error("Cannot find master node of compute group id %d", p_computeGroupId);
                // #endif /* LOGGER >= ERROR */
                return null;
            }
            masterNodeId = ChunkID.getCreatorID(tmp);
        }

        SubmitTaskRequest request = new SubmitTaskRequest(masterNodeId, p_taskScript);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending submit task script request to node 0x%X failed: %s", masterNodeId, e);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        SubmitTaskResponse response = (SubmitTaskResponse) request.getResponse();
        if (response.getStatusCode() != 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Error submitting task script, code %d", response.getStatusCode());
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // remember task for remote callbacks
        TaskScriptState state = new TaskScriptState(p_taskScript);
        state.assignTaskId(response.getAssignedPayloadId());
        state.setNodeIdSubmitted(m_boot.getNodeID());
        state.registerTaskListener(p_listener);
        m_remoteTasks.put(state.getTaskScriptIdAssigned(), state);

        // #if LOGGER >= INFO
        LOGGER.info("Submitted task to compute group %d with master node 0x%X: %s", p_computeGroupId, masterNodeId, p_taskScript);
        // #endif /* LOGGER >= INFO */

        return state;
    }

    @Override
    public void taskBeforeExecution(final TaskScriptState p_taskScriptState) {
        // only used for remote tasks to callback the node they were submitted on
        TaskExecutionStartedMessage message =
            new TaskExecutionStartedMessage(p_taskScriptState.getNodeIdSubmitted(), p_taskScriptState.getTaskScriptIdAssigned());

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending remote callback before execution to node %d failed", p_taskScriptState.getNodeIdSubmitted());
            // #endif /* LOGGER >= ERROR */
        }
    }

    @Override
    public void taskCompleted(final TaskScriptState p_taskScriptState) {
        // only used for remote tasks to callback the node they were submitted on
        TaskExecutionFinishedMessage message =
            new TaskExecutionFinishedMessage(p_taskScriptState.getNodeIdSubmitted(), p_taskScriptState.getTaskScriptIdAssigned(),
                p_taskScriptState.getExecutionReturnCodes());

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending remote callback completed to node 0x%X failed", p_taskScriptState.getNodeIdSubmitted());
            // #endif /* LOGGER >= ERROR */
        }

        // we don't have to remember this remote task anymore
        m_remoteTasks.remove(p_taskScriptState.getTaskScriptIdAssigned());
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
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
    protected boolean isServiceAccessor() {
        // we need this for the tasks
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_terminal = p_componentAccessor.getComponent(TerminalComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST, SubmitTaskRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE, SubmitTaskResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST,
            GetMasterStatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_RESPONSE,
            GetMasterStatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE,
            TaskExecutionStartedMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE,
            TaskExecutionFinishedMessage.class);

        m_network.register(SubmitTaskRequest.class, this);
        m_network.register(GetMasterStatusRequest.class, this);
        m_network.register(TaskExecutionStartedMessage.class, this);
        m_network.register(TaskExecutionFinishedMessage.class, this);

        switch (ComputeRole.toComputeRole(m_role)) {
            case MASTER:
                m_computeMSInstance =
                    new ComputeMaster(m_computeGroupId, m_pingInterval.getMs(), getServiceAccessor(), m_network, m_nameservice, m_boot, m_lookup);
                break;
            case SLAVE:
                m_computeMSInstance =
                    new ComputeSlave(m_computeGroupId, m_pingInterval.getMs(), getServiceAccessor(), m_network, m_nameservice, m_boot, m_lookup);
                break;
            case NONE:
                m_computeMSInstance = new ComputeNone(getServiceAccessor(), m_network, m_nameservice, m_boot, m_lookup);
                break;
            default:
                assert false;
                break;
        }

        registerTerminalCommands();

        // #if LOGGER >= INFO
        LOGGER.info("Started compute node type '%s' with compute group id %d", m_role, m_computeGroupId);
        // #endif /* LOGGER >= INFO */

        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_computeMSInstance.shutdown();
        m_computeMSInstance = null;

        return true;
    }

    /**
     * Register terminal commands
     */
    private void registerTerminalCommands() {
        m_terminal.registerTerminalCommand(new TcmdCompgrpls());
        m_terminal.registerTerminalCommand(new TcmdCompgrpstatus());
        m_terminal.registerTerminalCommand(new TcmdComptask());
        m_terminal.registerTerminalCommand(new TcmdComptaskscript());
    }

    /**
     * Handle an incoming SubmitTaskRequest
     *
     * @param p_request
     *     SubmitTaskRequest
     */
    private void incomingSubmitTaskRequest(final SubmitTaskRequest p_request) {
        SubmitTaskResponse response;

        // #if LOGGER >= DEBUG
        LOGGER.debug("Incoming remote submit task script request %s", p_request);
        // #endif /* LOGGER >= DEBUG */

        // check if we were able to create an instance (missing task class registration)
        if (p_request.getTaskScript() == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Creating instance for task script of request %s failed, most likely non registered task payload type", p_request);
            // #endif /* LOGGER >= ERROR */
            response = new SubmitTaskResponse(p_request, (short) -1, -1);
            response.setStatusCode((byte) 3);
            return;
        }

        if (m_computeMSInstance.getRole() != ComputeRole.MASTER) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot submit remote task script %s on non master node type", p_request.getTaskScript());
            // #endif /* LOGGER >= ERROR */

            response = new SubmitTaskResponse(p_request, (short) -1, -1);
            response.setStatusCode((byte) 1);
        } else {
            TaskScriptState taskScriptState = new TaskScriptState(p_request.getTaskScript());
            taskScriptState.assignTaskId(m_taskIdCounter.getAndIncrement());
            taskScriptState.setNodeIdSubmitted(p_request.getSource());
            taskScriptState.registerTaskListener(this);

            boolean ret = ((ComputeMaster) m_computeMSInstance).submitTask(taskScriptState);

            if (ret) {
                m_remoteTasks.put(taskScriptState.getTaskScriptIdAssigned(), taskScriptState);

                response = new SubmitTaskResponse(p_request, m_computeMSInstance.getComputeGroupId(), taskScriptState.getTaskScriptIdAssigned());
                response.setStatusCode((byte) 0);
            } else {
                response = new SubmitTaskResponse(p_request, (short) -1, -1);
                response.setStatusCode((byte) 2);
            }
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending response to submit task request to master %s failed", p_request);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Handle an incoming GetMasterStatusRequest
     *
     * @param p_request
     *     GetMasterStatusRequest
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

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending response to master status request %s failed", p_request);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Handle an incoming TaskExecutionStartedMessage
     *
     * @param p_message
     *     TaskExecutionStartedMessage
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
     *     TaskExecutionFinishedMessage
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

        /**
         * Constructor
         */
        public StatusMaster() {
            m_masterNodeId = NodeID.INVALID_ID;
            m_state = AbstractComputeMSBase.State.STATE_INVALID;
            m_connectedSlaves = new ArrayList<Short>();
            m_numTaskScriptsQueued = 0;
        }

        /**
         * Constructor
         *
         * @param p_masterNodeId
         *     Node id of the master
         * @param p_state
         *     The current state of the instance
         * @param p_connectedSlaves
         *     List of connected slave ids to this master.
         * @param p_numTaskScriptsQueued
         *     Number of task scripts queued currently on this master.
         * @param p_taskScriptsProcessed
         *     Number of task scripts processed so far.
         */
        StatusMaster(final short p_masterNodeId, final AbstractComputeMSBase.State p_state, final ArrayList<Short> p_connectedSlaves,
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
            m_masterNodeId = p_importer.readShort();
            m_state = AbstractComputeMSBase.State.values()[p_importer.readInt()];
            int size = p_importer.readInt();
            m_connectedSlaves = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                m_connectedSlaves.add(p_importer.readShort());
            }
            m_numTaskScriptsQueued = p_importer.readInt();
            m_taskScriptsProcessed = p_importer.readInt();
        }

        @Override
        public int sizeofObject() {
            return Short.BYTES + Integer.BYTES + Integer.BYTES + m_connectedSlaves.size() * Short.BYTES + Integer.BYTES + Integer.BYTES;
        }

        @Override
        public String toString() {
            String str = "";
            str += "Master: " + NodeID.toHexString(m_masterNodeId) + '\n';
            str += "State: " + m_state + '\n';
            str += "Task scripts queued: " + m_numTaskScriptsQueued + '\n';
            str += "Task scripts processed: " + m_taskScriptsProcessed + '\n';
            str += "Connected slaves(" + m_connectedSlaves.size() + "):\n";
            for (int i = 0; i < m_connectedSlaves.size(); i++) {
                str += i + ": " + NodeID.toHexString(m_connectedSlaves.get(i));
                if (!m_connectedSlaves.isEmpty() && i < m_connectedSlaves.size() - 1) {
                    str += "\n";
                }
            }
            return str;
        }
    }
}
