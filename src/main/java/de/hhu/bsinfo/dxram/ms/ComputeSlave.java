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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.ms.messages.ExecuteTaskScriptRequest;
import de.hhu.bsinfo.dxram.ms.messages.ExecuteTaskScriptResponse;
import de.hhu.bsinfo.dxram.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxram.ms.messages.SignalMessage;
import de.hhu.bsinfo.dxram.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxram.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNode;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeAbort;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeData;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeResultCondition;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeResultSwitch;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Implementation of a slave. The slave waits for tasks for execution from the master.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
class ComputeSlave extends AbstractComputeMSBase implements MessageReceiver, TaskSignalInterface {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ComputeSlave.class);

    private short m_masterNodeId = NodeID.INVALID_ID;

    private volatile TaskScript m_taskScript;
    private volatile TaskContextData m_ctxData;
    private Lock m_executeTaskScriptLock = new ReentrantLock(false);
    private Lock m_handleSignalLock = new ReentrantLock(false);

    private int m_masterExecutionBarrierId;

    /**
     * Constructor.
     *
     * @param p_computeGroupId
     *         Compute group id the instance is assigned to.
     * @param p_pingIntervalMs
     *         Ping interval in ms to check back with the compute group if still alive.
     * @param p_serviceAccessor
     *         Service accessor for tasks.
     * @param p_network
     *         NetworkComponent
     * @param p_nameservice
     *         NameserviceComponent
     * @param p_boot
     *         BootComponent
     * @param p_lookup
     *         LookupComponent
     * @param p_plugin
     *         PluginComponent
     */
    ComputeSlave(final short p_computeGroupId, final long p_pingIntervalMs,
            final DXRAMServiceAccessor p_serviceAccessor, final NetworkComponent p_network,
            final NameserviceComponent p_nameservice, final AbstractBootComponent p_boot,
            final LookupComponent p_lookup, final PluginComponent p_plugin) {
        super(ComputeRole.SLAVE, p_computeGroupId, p_pingIntervalMs, p_serviceAccessor, p_network, p_nameservice,
                p_boot, p_lookup, p_plugin);

        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST, SlaveJoinRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE, SlaveJoinResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST,
                ExecuteTaskScriptRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE,
                ExecuteTaskScriptResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SIGNAL_MESSAGE, SignalMessage.class);

        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST, this);
        m_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SIGNAL_MESSAGE,
                this);

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
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST:
                        incomingExecuteTaskScriptRequest((ExecuteTaskScriptRequest) p_message);
                        break;
                    case MasterSlaveMessages.SUBTYPE_SIGNAL_MESSAGE:
                        incomingSignalMessage((SignalMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    public void sendSignalToMaster(final Signal p_signal) {

        try {
            m_network.sendMessage(new SignalMessage(m_masterNodeId, p_signal));
        } catch (final NetworkException e) {

            LOGGER.error("Sending signal to master 0x%X failed: %s", m_masterNodeId, e);

        }
    }

    /**
     * Setup state of the slave. Connect to the master of the compute group assigend to.
     */
    private void stateSetup() {

        LOGGER.debug("Setting up slave for compute group %d", m_computeGroupId);

        // bootstrap: get master node id from nameservice
        if (m_masterNodeId == NodeID.INVALID_ID) {
            long tmp = m_nameservice.getChunkID(m_nameserviceMasterNodeIdKey, 0);
            if (tmp == -1) {
                LOGGER.error("Setting up slave, cannot find nameservice entry for master node id for key %s " +
                        "of compute group %d", m_nameserviceMasterNodeIdKey, m_computeGroupId);

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

        try {
            m_network.sendSync(request);

            SlaveJoinResponse response = (SlaveJoinResponse) request.getResponse();
            if (response.getStatus() != 0) {
                // master is busy, retry
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {
                }
            } else {
                LOGGER.info("Successfully joined compute group %d with master 0x%X", m_computeGroupId, m_masterNodeId);

                m_masterExecutionBarrierId = response.getExecutionBarrierId();
                m_state = State.STATE_IDLE;

                LOGGER.debug("Entering idle state");
            }
        } catch (final NetworkException e) {
            LOGGER.error("Sending join request to master 0x%X failed: %s", m_masterNodeId, e);

            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ignored) {
            }

            // trigger a full retry. might happen that the master node has changed
            m_masterNodeId = NodeID.INVALID_ID;
        }
    }

    /**
     * Idle state. Connected to the master of the compute group. Ping and wait for instructions.
     */
    private void stateIdle() {
        if (m_taskScript != null) {
            m_state = State.STATE_EXECUTE;
        } else {
            // check periodically if master is still available
            if (m_lastPingMs + m_pingIntervalMs < System.currentTimeMillis()) {
                if (!m_boot.isNodeOnline(m_masterNodeId)) {
                    // master is gone, go back to sign on
                    LOGGER.info("Master 0x%X went offline, logout", m_masterNodeId);

                    m_masterNodeId = NodeID.INVALID_ID;
                    m_state = State.STATE_SETUP;
                    return;
                }

                m_lastPingMs = System.currentTimeMillis();

                LOGGER.trace("Pinging master 0x%X: online", m_masterNodeId);
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
        LOGGER.info("Starting execution of task script %s", m_taskScript);

        m_executeTaskScriptLock.lock();

        Integer result = 0;
        for (TaskScriptNode node : m_taskScript.getTasks()) {
            result = executeTaskScriptNode(node, result);

            if (result == null) {
                result = 0;
                break;
            }
        }

        LOGGER.info("Execution finished, return code: %d", result);

        m_handleSignalLock.lock();
        m_taskScript = null;
        m_executeTaskScriptLock.unlock();
        m_handleSignalLock.unlock();

        // set idle state before sync to avoid race condition with master sending
        // new tasks right after the sync
        m_state = State.STATE_IDLE;

        // sync until the master tells us we are done. there might be other slaves
        // taking a different path in the task script thus taking longer to finish
        Long masterRetCode;
        do {
            LOGGER.debug("Final syncing with master 0x%X ...", m_masterNodeId);

            BarrierStatus barrierResult = m_lookup.barrierSignOn(m_masterExecutionBarrierId, result);

            masterRetCode = barrierResult.findCustomData(m_masterNodeId);

            if (masterRetCode == null) {
                throw new RuntimeException("Could not find master return code in barrier sign on result");
            }
        } while (masterRetCode != 0);

        LOGGER.debug("Syncing done, entering idle state");
    }

    /**
     * Execute a sync step with the master. This is called after execution
     * of a single task statement of a task script.
     */
    private void syncStepMaster() {
        LOGGER.debug("Sync step with master 0x%X ...", m_masterNodeId);

        m_lookup.barrierSignOn(m_masterExecutionBarrierId, 1L << 32);
    }

    /**
     * Execute the give task script node.
     *
     * @param p_taskScriptNode
     *         Script node to execute
     * @param p_prevReturnCode
     *         Return code of the previous task executed
     * @return Return value of the node executed
     */
    private Integer executeTaskScriptNode(final TaskScriptNode p_taskScriptNode, final Integer p_prevReturnCode) {
        Integer result = p_prevReturnCode;

        if (result != null) {
            if (p_taskScriptNode instanceof TaskScriptNodeResultCondition) {
                TaskScriptNodeResultCondition condition = (TaskScriptNodeResultCondition) p_taskScriptNode;

                LOGGER.debug("Executing condition: %s", condition);

                TaskScript script = condition.evaluate(result);
                syncStepMaster();

                for (TaskScriptNode node : script.getTasks()) {
                    result = executeTaskScriptNode(node, result);
                    if (result == null) {
                        break;
                    }
                }
            } else if (p_taskScriptNode instanceof Task) {
                Task task = (Task) p_taskScriptNode;

                LOGGER.debug("Executing task: %s", task);

                try {
                    result = task.execute(new TaskContext(m_ctxData, this, getServiceAccessor()));
                } catch (final Exception e) {
                    LOGGER.error("Executing task failed, exception", e);
                    result = -1;
                }
                syncStepMaster();
            } else if (p_taskScriptNode instanceof TaskScriptNodeResultSwitch) {
                TaskScriptNodeResultSwitch resSwitch = (TaskScriptNodeResultSwitch) p_taskScriptNode;

                LOGGER.debug("Executing switch: %s", resSwitch);

                TaskScript script = resSwitch.evaluate(result);
                syncStepMaster();

                for (TaskScriptNode node : script.getTasks()) {
                    result = executeTaskScriptNode(node, result);
                    if (result == null) {
                        break;
                    }
                }
            } else if (p_taskScriptNode instanceof TaskScriptNodeAbort) {
                TaskScriptNodeAbort abort = (TaskScriptNodeAbort) p_taskScriptNode;

                LOGGER.debug("Executing abort: %s", abort);

                System.out.printf("Aborting task script: %s\n", abort.getAbortMsg());
                result = null;
            } else if (p_taskScriptNode instanceof TaskScriptNodeData) {
                throw new IllegalStateException("Script node type TaskScriptNodeData not allowed here. Must be " +
                        "converted to proper task instances");
            } else {
                throw new RuntimeException("Unhandled script node type " + p_taskScriptNode.getClass().getName());
            }
        }

        return result;
    }

    /**
     * Handle an incoming ExecuteTaskScriptScriptRequest.
     *
     * @param p_message
     *         ExecuteTaskScriptScriptRequest
     */
    private void incomingExecuteTaskScriptRequest(final ExecuteTaskScriptRequest p_message) {
        ExecuteTaskScriptResponse response;
        TaskScript taskScript = null;

        if (m_executeTaskScriptLock.tryLock() && m_state == State.STATE_IDLE) {
            taskScript = p_message.getTaskScript();

            // complete reflection of generic task objects
            reflectIncomingNodeDataInstances(taskScript);

            response = new ExecuteTaskScriptResponse(p_message, (byte) 0);

            m_executeTaskScriptLock.unlock();
        } else {
            // cannot execute task, invalid state
            response = new ExecuteTaskScriptResponse(p_message, (byte) 1);
        }

        try {
            m_network.sendMessage(response);

            if (taskScript != null) {
                m_ctxData = p_message.getTaskContextData();

                // assign and start execution if non null
                m_taskScript = taskScript;
            }
        } catch (final NetworkException e) {

            LOGGER.error("Sending response for executing task to 0x%X failed: %s", p_message.getSource(), e);

        }
    }

    /**
     * Handle a SignalMessage.
     *
     * @param p_message
     *         SignalMessage
     */
    private void incomingSignalMessage(final SignalMessage p_message) {
        m_handleSignalLock.lock();
        if (m_taskScript != null) {
            //m_tas.handleSignal(p_message.getSignal());
        }

        // TODO add signal to a queue otherwise it might get lost between two tasks of a script

        m_handleSignalLock.unlock();
    }
}
