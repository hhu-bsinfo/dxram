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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Implementation of a master. The master accepts tasks, pushes them to a queue and distributes them
 * to the conencted slaves for execution.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
class ComputeMaster extends AbstractComputeMSBase implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ComputeMaster.class);

    private static final int MAX_TASK_COUNT = 100;

    private List<Short> m_signedOnSlaves = new ArrayList<>();
    private Lock m_joinLock = new ReentrantLock(false);
    private ConcurrentLinkedQueue<TaskScriptState> m_taskScripts = new ConcurrentLinkedQueue<>();
    private AtomicInteger m_taskCount = new AtomicInteger(0);
    private int m_executeBarrierIdentifier;
    private int m_executionBarrierId;

    private volatile int m_taskScriptsProcessed;

    /**
     * Constructor.
     *
     * @param p_computeGroupId
     *         Compute group id the instance is assigned to.
     * @param p_pingIntervalMs
     *         Ping interval in ms to check back with the compute group if still alive.
     * @param p_serviceAccessor
     *         Accessor to services for compute tasks.
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
    ComputeMaster(final short p_computeGroupId, final long p_pingIntervalMs,
            final DXRAMServiceAccessor p_serviceAccessor, final NetworkComponent p_network,
            final NameserviceComponent p_nameservice, final AbstractBootComponent p_boot,
            final LookupComponent p_lookup, final PluginComponent p_plugin) {
        super(ComputeRole.MASTER, p_computeGroupId, p_pingIntervalMs, p_serviceAccessor, p_network, p_nameservice,
                p_boot, p_lookup, p_plugin);

        p_network.register(DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST,
                this);

        m_executionBarrierId = m_lookup.barrierAllocate(1);

        start();
    }

    /**
     * Get a list of currently connected salves.
     *
     * @return List of currently connected slaves (node ids).
     */
    ArrayList<Short> getConnectedSlaves() {
        return new ArrayList<>(m_signedOnSlaves);
    }

    /**
     * Submit a task to this master.
     *
     * @param p_taskScriptState
     *         TaskScriptState containing the script to submit.
     * @return True if submission was successful, false if the max number of tasks queued is reached.
     */
    boolean submitTask(final TaskScriptState p_taskScriptState) {
        if (m_taskCount.get() < MAX_TASK_COUNT) {
            m_taskScripts.add(p_taskScriptState);
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
     * Get the total amount of task scripts processed so far.
     *
     * @return Number of tasks processed.
     */
    int getTotalTaskScriptsProcessed() {
        return m_taskScriptsProcessed;
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
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST:
                        incomingSlaveJoinRequest((SlaveJoinRequest) p_message);
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

    /**
     * Error state. Entered if an error happened and we can't recover.
     */
    private static void stateErrorDie() {

        LOGGER.error("Master error state");

        try {
            Thread.sleep(1000);
        } catch (final InterruptedException ignored) {
        }
    }

    /**
     * Setup state. Register node id in the nameservice to allow slaves to discover this master.
     */
    private void stateSetup() {

        LOGGER.info("Setting up master of compute group %d", m_computeGroupId);

        // check first, if there is already a master registered for this compute group
        long id = m_nameservice.getChunkID(m_nameserviceMasterNodeIdKey, 0);
        if (id != -1) {

            LOGGER.error("Cannot setup master for compute group id %d, node 0x%X is already master of group",
                    m_computeGroupId, ChunkID.getCreatorID(id));

            m_state = State.STATE_ERROR_DIE;
            return;
        }

        // setup bootstrapping for other slaves
        // use the nameservice to store our node id
        m_nameservice.register(ChunkID.getChunkID(m_boot.getNodeId(), ChunkID.INVALID_ID),
                m_nameserviceMasterNodeIdKey);

        m_state = State.STATE_IDLE;

        LOGGER.debug("Entering idle state");

    }

    /**
     * Idle state. Wait for slaves to sign on and for tasks to be submitted. Also ping and check if slaves
     * are still available and remove them from the group if not.
     */
    private void stateIdle() {
        if (m_taskCount.get() > 0) {
            if (m_signedOnSlaves.size() < 1) {

                LOGGER.warn("Got %d tasks queued but no slaves", m_taskCount.get());

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

        // get next taskScript
        m_taskCount.decrementAndGet();
        TaskScriptState taskScriptState = m_taskScripts.poll();
        TaskScript taskScript = taskScriptState.getTaskScript();
        if (taskScript == null) {

            LOGGER.error("Cannot proceed with task script state %s, missing script", taskScriptState);

            m_state = State.STATE_IDLE;
            return;
        }

        int minSlaves = taskScript.getMinSlaves();
        int maxSlaves = taskScript.getMaxSlaves();

        if (maxSlaves == TaskScript.NUM_SLAVES_ARBITRARY) {
            maxSlaves = NodeID.MAX_ID;
        }

        while (m_signedOnSlaves.size() < minSlaves || m_signedOnSlaves.size() > maxSlaves) {

            LOGGER.debug("Waiting for num slaves in interval [%d, %d] for task script %s (current slave count: %d)...",
                    minSlaves, maxSlaves, taskScript,
                    m_signedOnSlaves.size());

            try {
                Thread.sleep(2000);
            } catch (final InterruptedException ignored) {
            }

            // bad but might happen that a slave goes offline
            checkAllSlavesOnline();
        }

        // lock joining of further slaves
        m_joinLock.lock();

        LOGGER.info("Starting execution of task script %s with %d slaves", taskScript, m_signedOnSlaves.size());

        short[] slaves = new short[m_signedOnSlaves.size()];
        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = m_signedOnSlaves.get(i);
        }

        taskScriptState.notifyListenersExecutionStarts();

        // send task script to slaves
        short numberOfSlavesOnExecution = 0;
        // avoid clashes with other compute groups, but still alter the flag on every next sync
        m_executeBarrierIdentifier = (m_executeBarrierIdentifier + 1) % 2 + m_computeGroupId * 2;
        for (short slave : slaves) {
            TaskContextData ctxData = new TaskContextData(m_computeGroupId, numberOfSlavesOnExecution, slaves);

            // pass barrier identifier for syncing after taskScript along
            ExecuteTaskScriptRequest request = new ExecuteTaskScriptRequest(slave, m_executeBarrierIdentifier, ctxData,
                    taskScript);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {

                LOGGER.error("Sending task to slave 0x%X failed: %s", slave, e);

                // remove slave from list
                m_signedOnSlaves.remove(slave);
                continue;
            }

            ExecuteTaskScriptResponse response = (ExecuteTaskScriptResponse) request.getResponse();
            if (response.getStatus() != 0) {
                // exclude slave from execution

                LOGGER.error("Slave 0x%X response %d on execution of task script %s excluding from current execution",
                        slave, response.getStatus(), taskScript);

            } else {
                numberOfSlavesOnExecution++;
            }
        }

        LOGGER.debug("Executing sync steps with %d/%d slaves...", numberOfSlavesOnExecution, m_signedOnSlaves.size());

        int[] returnCodes;
        do {

            LOGGER.debug("Awaiting sync step...");

            BarrierStatus result = m_lookup.barrierSignOn(m_executionBarrierId, -1);

            if (result != null) {

                LOGGER.debug("Sync step done");

                final boolean[] allDone = {true};
                result.forEachSignedOnPeer((p_signedOnPeer, p_customData) -> {
                    if ((int) (p_customData >> 32L) > 0) {
                        allDone[0] = false;
                    }
                });

                if (!allDone[0]) {
                    continue;
                }

                // one last sync step to tell the slaves everyone finished
                result = m_lookup.barrierSignOn(m_executionBarrierId, 0);

                if (result != null) {
                    // grab return codes from barrier
                    returnCodes = new int[slaves.length];

                    result.forEachSignedOnPeer((p_signedOnPeer, p_customData) -> {
                        // sort them to match the indices of the slave list
                        for (int i = 0; i < slaves.length; i++) {
                            if (p_signedOnPeer == slaves[i]) {
                                returnCodes[i] = (int) p_customData;
                            }
                        }
                    });
                } else {
                    returnCodes = new int[slaves.length];
                    for (int i = 0; i < returnCodes.length; i++) {
                        returnCodes[i] = -1;
                    }
                }
            } else {
                returnCodes = new int[slaves.length];
                for (int i = 0; i < returnCodes.length; i++) {
                    returnCodes[i] = -1;
                }
            }

            LOGGER.debug("Sync all done");

            break;
        } while (true);

        taskScriptState.notifyListenersExecutionCompleted(returnCodes);

        m_taskScriptsProcessed++;

        m_state = State.STATE_IDLE;
        // allow further slaves to join
        m_joinLock.unlock();

        LOGGER.debug("Entering idle state");

    }

    /**
     * Check online status of all slaves (once).
     */
    private void checkAllSlavesOnline() {
        // check if slaves are still alive
        List<Short> onlineNodesList = m_boot.getOnlineNodeIds();

        m_joinLock.lock();
        Iterator<Short> it = m_signedOnSlaves.iterator();
        while (it.hasNext()) {
            short slave = it.next();
            if (!onlineNodesList.contains(slave)) {

                LOGGER.info("Slave 0x%X is not available anymore, removing", slave);

                it.remove();
            }
        }
        m_joinLock.unlock();

        m_lastPingMs = System.currentTimeMillis();

        LOGGER.trace("Pinging slaves, %d online", m_signedOnSlaves.size());

    }

    /**
     * Handle a SlaveJoinRequest
     *
     * @param p_message
     *         SlaveJoinRequest
     */
    private void incomingSlaveJoinRequest(final SlaveJoinRequest p_message) {
        if (m_joinLock.tryLock()) {
            if (m_signedOnSlaves.contains(p_message.getSource())) {

                LOGGER.warn("Joining slave, already joined: 0x%X", p_message.getSource());

            } else {
                m_signedOnSlaves.add(p_message.getSource());

                // expand barrier, +1 for the master
                m_lookup.barrierChangeSize(m_executionBarrierId, m_signedOnSlaves.size() + 1);
            }

            SlaveJoinResponse response = new SlaveJoinResponse(p_message, m_executionBarrierId, (byte) 0);
            try {
                m_network.sendMessage(response);

                LOGGER.info("Slave (%d) 0x%X has joined", m_signedOnSlaves.size() - 1, p_message.getSource());
            } catch (final NetworkException e) {
                LOGGER.error("Sending response to join request of slave 0x%X failed: %s", p_message.getSource(), e);

                // remove slave
                m_signedOnSlaves.remove(p_message.getSource());
            }

            m_joinLock.unlock();
        } else {

            LOGGER.trace("Cannot join slave, master not in idle state");

            // send response that joining is not possible currently
            SlaveJoinResponse response = new SlaveJoinResponse(p_message, BarrierID.INVALID_ID, (byte) 1);
            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                LOGGER.error("Sending response to join request of slave 0x%X failed: %s", p_message.getSource(), e);
            }
        }
    }

    /**
     * Handle a SignalMessage
     *
     * @param p_message
     *         SignalMessage
     */
    private void incomingSignalMessage(final SignalMessage p_message) {
        switch (p_message.getSignal()) {
            case SIGNAL_ABORT: {
                // the slave requested aborting the currently running task
                // send an abort to all other slaves as well
                for (short slaveNodeId : m_signedOnSlaves) {
                    try {
                        m_network.sendMessage(new SignalMessage(slaveNodeId, p_message.getSignal()));
                    } catch (final NetworkException e) {
                        LOGGER.error("Sending signal to slave 0x%X failed: %s", p_message.getSource(), e);
                    }
                }

                break;
            }

            default: {
                LOGGER.error("Unhandled signal %d from peer 0x%X", p_message.getSignal(), p_message.getSource());
                break;
            }
        }
    }
}
