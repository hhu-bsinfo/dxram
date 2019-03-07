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

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.ms.messages.ExecuteTaskScriptRequest;
import de.hhu.bsinfo.dxram.ms.messages.ExecuteTaskScriptResponse;
import de.hhu.bsinfo.dxram.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxram.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxram.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNode;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeData;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeResultCondition;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeResultSwitch;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;

/**
 * Base class for the master slave compute framework.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
abstract class AbstractComputeMSBase extends Thread {
    private final Logger LOGGER;

    static final String NAMESERVICE_ENTRY_IDENT = "MAS";
    private static final byte MIN_COMPUTE_GROUP_ID = -1;
    static final byte MAX_COMPUTE_GROUP_ID = 99;

    /**
     * States of the master/slave instances
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.02.2016
     */
    enum State {
        STATE_INVALID, STATE_SETUP, STATE_IDLE, STATE_EXECUTE, STATE_ERROR_DIE, STATE_TERMINATE,
    }

    private DXRAMServiceAccessor m_serviceAccessor;

    @SuppressWarnings("checkstyle")
    protected NetworkComponent m_network;
    protected NameserviceComponent m_nameservice;
    protected AbstractBootComponent m_boot;
    protected LookupComponent m_lookup;
    protected PluginComponent m_plugin;

    protected volatile State m_state = State.STATE_SETUP;
    protected ComputeRole m_role;
    protected short m_computeGroupId;
    protected long m_pingIntervalMs;
    protected long m_lastPingMs;
    protected String m_nameserviceMasterNodeIdKey;

    /**
     * Constructor
     *
     * @param p_role
     *         Compute role of the instance.
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
     *         PluginCompoennt
     */
    AbstractComputeMSBase(final ComputeRole p_role, final short p_computeGroupId, final long p_pingIntervalMs,
            final DXRAMServiceAccessor p_serviceAccessor, final NetworkComponent p_network,
            final NameserviceComponent p_nameservice, final AbstractBootComponent p_boot,
            final LookupComponent p_lookup, final PluginComponent p_plugin) {
        super("ComputeMS-" + p_role + '-' + p_computeGroupId);

        LOGGER = LogManager.getFormatterLogger(getClass());

        m_role = p_role;
        m_computeGroupId = p_computeGroupId;
        m_pingIntervalMs = p_pingIntervalMs;
        assert m_computeGroupId >= MIN_COMPUTE_GROUP_ID && m_computeGroupId <= MAX_COMPUTE_GROUP_ID;
        m_nameserviceMasterNodeIdKey = NAMESERVICE_ENTRY_IDENT + m_computeGroupId;

        m_serviceAccessor = p_serviceAccessor;

        m_network = p_network;
        m_nameservice = p_nameservice;
        m_boot = p_boot;
        m_lookup = p_lookup;
        m_plugin = p_plugin;

        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST, SlaveJoinRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE, SlaveJoinResponse.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                        MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST, ExecuteTaskScriptRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE,
                ExecuteTaskScriptResponse.class);
    }

    /**
     * Shut down this compute node.
     */
    public abstract void shutdown();

    /**
     * Use the plugin manager to replace any generic data nodes with proper node instances. This is required
     * after receiving a task script from a remote node.
     *
     * @param p_taskScript
     *         Task script to process and apply reflection to generic task node instances.
     */
    public void reflectIncomingNodeDataInstances(final TaskScript p_taskScript) {
        TaskScriptNode[] tasks = p_taskScript.getTasks();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i] instanceof TaskScriptNodeData) {
                TaskScriptNodeData genericNode = (TaskScriptNodeData) tasks[i];

                Class clazz;

                try {
                    clazz = m_plugin.getClassByName(genericNode.getName());
                } catch (final ClassNotFoundException e) {
                    throw new IllegalStateException(e);
                }

                try {
                    tasks[i] = (TaskScriptNode) clazz.getConstructor().newInstance();
                } catch (final InstantiationException | IllegalAccessException | InvocationTargetException |
                        NoSuchMethodException e) {
                    throw new IllegalStateException(e);
                }

                // de-serialize byte array data to object
                if (genericNode.getData().length > 0) {
                    ByteBuffer buffer = ByteBuffer.wrap(genericNode.getData());
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    ByteBufferImExporter importer = new ByteBufferImExporter(buffer);
                    importer.importObject(tasks[i]);
                }
            } else if (tasks[i] instanceof TaskScriptNodeResultCondition) {
                TaskScriptNodeResultCondition conditionNode = (TaskScriptNodeResultCondition) tasks[i];

                reflectIncomingNodeDataInstances(conditionNode.getScriptTrueCase());
                reflectIncomingNodeDataInstances(conditionNode.getScriptFalseCase());
            } else if (tasks[i] instanceof TaskScriptNodeResultSwitch) {
                TaskScriptNodeResultSwitch caseNode = (TaskScriptNodeResultSwitch) tasks[i];

                reflectIncomingNodeDataInstances(caseNode.getDefaultSwitchCase().getScriptCase());

                for (TaskScriptNodeResultSwitch.Case caze : caseNode.getSwitchCases()) {
                    reflectIncomingNodeDataInstances(caze.getScriptCase());
                }
            }
        }
    }

    /**
     * Get the compute role assigned to this instance.
     *
     * @return Compute role assigned.
     */
    ComputeRole getRole() {
        return m_role;
    }

    /**
     * Get the current state.
     *
     * @return State of the instance.
     */
    State getComputeState() {
        return m_state;
    }

    /**
     * Get the compute group id this node is assigend to.
     *
     * @return Compute group id assigned to.
     */
    short getComputeGroupId() {
        return m_computeGroupId;
    }

    /**
     * Get the service accessor of DXRAM to be passed to the tasks being executed
     *
     * @return DXRAMService accessor
     */
    DXRAMServiceAccessor getServiceAccessor() {
        return m_serviceAccessor;
    }
}
