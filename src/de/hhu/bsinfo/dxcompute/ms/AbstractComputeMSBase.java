/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxcompute.ms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Base class for the master slave compute framework.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
abstract class AbstractComputeMSBase extends Thread {

    private final Logger LOGGER;

    static final String NAMESERVICE_ENTRY_IDENT = "MAS";
    private static final byte MIN_COMPUTE_GROUP_ID = -1;
    static final byte MAX_COMPUTE_GROUP_ID = 99;

    /**
     * States of the master/slave instances
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

    protected volatile State m_state = State.STATE_SETUP;
    protected ComputeRole m_role;
    protected short m_computeGroupId = -1;
    protected long m_pingIntervalMs;
    protected long m_lastPingMs;
    protected String m_nameserviceMasterNodeIdKey;

    /**
     * Constructor
     * @param p_role
     *            Compute role of the instance.
     * @param p_computeGroupId
     *            Compute group id the instance is assigned to.
     * @param p_pingIntervalMs
     *            Ping interval in ms to check back with the compute group if still alive.
     * @param p_serviceAccessor
     *            Service accessor for tasks.
     * @param p_network
     *            NetworkComponent
     * @param p_nameservice
     *            NameserviceComponent
     * @param p_boot
     *            BootComponent
     * @param p_lookup
     *            LookupComponent
     */
    AbstractComputeMSBase(final ComputeRole p_role, final short p_computeGroupId, final long p_pingIntervalMs, final DXRAMServiceAccessor p_serviceAccessor,
            final NetworkComponent p_network, final NameserviceComponent p_nameservice, final AbstractBootComponent p_boot, final LookupComponent p_lookup) {
        super("ComputeMS-" + p_role + "-" + p_computeGroupId);

        LOGGER = LogManager.getFormatterLogger(this.getClass().getSimpleName());

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

        m_network.registerMessageType(DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST, SlaveJoinRequest.class);
        m_network
                .registerMessageType(DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE, SlaveJoinResponse.class);
        m_network.registerMessageType(DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST,
                ExecuteTaskRequest.class);
        m_network.registerMessageType(DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE,
                ExecuteTaskResponse.class);
    }

    /**
     * Get the compute role assigned to this instance.
     * @return Compute role assigned.
     */
    ComputeRole getRole() {
        return m_role;
    }

    /**
     * Get the current state.
     * @return State of the instance.
     */
    State getComputeState() {
        return m_state;
    }

    /**
     * Get the compute group id this node is assigend to.
     * @return Compute group id assigned to.
     */
    short getComputeGroupId() {
        return m_computeGroupId;
    }

    @Override
    public abstract void run();

    /**
     * Shut down this compute node.
     */
    public abstract void shutdown();

    /**
     * Get the service accessor of DXRAM to be passed to the tasks being executed
     * @return DXRAMService accessor
     */
    DXRAMServiceAccessor getServiceAccessor() {
        return m_serviceAccessor;
    }
}
