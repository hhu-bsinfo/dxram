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

package de.hhu.bsinfo.dxram.failure;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.failure.messages.FailureMessages;
import de.hhu.bsinfo.dxram.failure.messages.FailureRequest;
import de.hhu.bsinfo.dxram.failure.messages.FailureResponse;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Handles a node failure.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2016
 */
public class FailureComponent extends AbstractDXRAMComponent implements MessageReceiver, EventListener<AbstractEvent> {

    private static final Logger LOGGER = LogManager.getFormatterLogger(FailureComponent.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private LookupComponent m_lookup;
    private EventComponent m_event;
    private NetworkComponent m_network;

    private byte[] m_nodeStatus;
    private ReentrantLock m_failureLock;

    /**
     * Creates the failure component
     */
    public FailureComponent() {
        super(DXRAMComponentOrder.Init.FAILURE, DXRAMComponentOrder.Shutdown.FAILURE);

        m_nodeStatus = new byte[Short.MAX_VALUE * 2];
        m_failureLock = new ReentrantLock(false);
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {

        if (p_event instanceof ConnectionLostEvent) {
            ConnectionLostEvent event = (ConnectionLostEvent) p_event;
            short nodeID = event.getNodeID();

            if (nodeID != NodeID.INVALID_ID) {
                m_failureLock.lock();
                if (m_nodeStatus[nodeID & 0xFFFF] < 5) {
                    m_nodeStatus[nodeID & 0xFFFF] = 5;
                    m_failureLock.unlock();

                    /*
                     * Section A - High priority
                     * A connection was removed after an error -> try to re-connect
                     * If re-connecting fails, failure will be handled
                     *
                     * There can only be one thread per NodeID in here, but there might be another thread in section B for
                     * the same NodeID. All other events (ConnectionLostEvents and ResponseDelayedEvents) for given NodeID
                     * are ignored meanwhile. If there is a thread in section B for given NodeID, this section is still
                     * reachable.
                     */

                    // #if LOGGER == DEBUG
                    LOGGER.debug("ConnectionLostEvent triggered: 0x%X", nodeID);
                    // #endif /* LOGGER == DEBUG */

                    try {
                        m_network.connectNode(nodeID);

                        // #if LOGGER == DEBUG
                        LOGGER.debug("Re-connect successful, continuing");
                        // #endif /* LOGGER == DEBUG */
                    } catch (final NetworkException e) {
                        // #if LOGGER == DEBUG
                        LOGGER.debug("Node is unreachable. Initiating failure handling");
                        // #endif /* LOGGER == DEBUG */

                        failureHandling(nodeID);
                    }

                    m_failureLock.lock();
                    m_nodeStatus[nodeID & 0xFFFF] = 0;
                    m_failureLock.unlock();
                } else {
                    m_failureLock.unlock();
                }
            }
        } else {
            ResponseDelayedEvent event = (ResponseDelayedEvent) p_event;
            short nodeID = event.getNodeID();

            if (nodeID != NodeID.INVALID_ID) {
                m_failureLock.lock();
                if (m_nodeStatus[nodeID & 0xFFFF] == 0) {
                    m_nodeStatus[nodeID & 0xFFFF]++;
                    m_failureLock.unlock();

                    /*
                     * Section B - Low priority
                     * A response was delayed -> send a message to check connection
                     *
                     * There can only be one thread per NodeID in here, but there might be another thread in section A for
                     * the same NodeID. All other ResponseDelayedEvents for given NodeID are ignored meanwhile. If there is
                     * a thread in section A for given NodeID, this section is unreachable.
                     */

                    // #if LOGGER == DEBUG
                    LOGGER.debug("ResponseDelayedEvent triggered: 0x%X. Sending default message and return.", nodeID);
                    // #endif /* LOGGER == DEBUG */

                    try {
                        // Sending default message to detect connection failure. If the connection is broken,
                        // a ConnectionLostEvent will be triggered
                        m_network.sendMessage(new DefaultMessage(nodeID));
                    } catch (final NetworkException ignored) {
                    }

                    m_failureLock.lock();
                    m_nodeStatus[nodeID & 0xFFFF]--;
                    m_failureLock.unlock();
                } else {
                    m_failureLock.unlock();
                }
            }
        }

    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.FAILURE_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case FailureMessages.SUBTYPE_FAILURE_REQUEST:
                        incomingFailureRequest((FailureRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_REQUEST, FailureRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_RESPONSE, FailureResponse.class);
        m_network.register(FailureRequest.class, this);

        m_event.registerListener(this, ConnectionLostEvent.class);
        m_event.registerListener(this, ResponseDelayedEvent.class);

        return true;
    }

    // --------------------------------------------------------------------------------

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    /**
     * Dispatcher for a node failure
     *
     * @param p_nodeID
     *     NodeID of failed node
     */
    private void failureHandling(final short p_nodeID) {
        boolean responsible;
        NodeRole ownRole;
        NodeRole roleOfFailedNode;

        ownRole = m_boot.getNodeRole();
        roleOfFailedNode = m_boot.getNodeRole(p_nodeID);

        /*
         * Things to do after a node failure ("informing" a node means the node gets here, too):
         *  1) This is a superpeer:
         *      a) Failed node was a superpeer:
         *          In LookupComponent:
         *          i)   This is the successor: Inform all other superpeers actively and take over peers (other metadata is already available)
         *          ii)  This is the last backup superpeer: Send failed superpeer's metadata to this superpeer's successor
         *          iii) Failed superpeer was one of this node's backup superpeers: Send this superpeer's metadata to new, succeeding backup superpeer
         *          iv)  Remove superpeer from ring (locally, but as every superpeer does this the node is erased completely)
         *          v)   This is the successor or predecessor: Determine new neighbor (is a special role in overlay)
         *          In ZooKeeperBootComponent:
         *          i)   This is the successor: Remove superpeer from "/nodes/superpeers"
         *          ii)  This is the successor: Add ChunkID of failed superpeer to "/nodes/free" if node was not registered in file at startup
         *          iii) This is the successor: Replace failed superpeer as a bootstrap ("/nodes/bootstrap") if it was the bootstrap
         *          iv)  This is the successor: If superpeer was not registered in file at startup, remove its ChunkID from "/nodes/new"
         *      b) Failed node was a peer:
         *          In LookupComponent:
         *          i)   Inform all assigned peers
         *          ii)  This is the responsible superpeer: Inform all other superpeers (to inform all peers, see above)
         *          iii) This is the responsible superpeer: Get all backup ranges (normal and migration) from LookupTree and start recovery for every backup
         *                 range by sending a RecoverBackupRangeRequest to the primary backup peers (secondary and tertiary if unavailable)
         *                 - If the recovery was successful: Set the new owner as restorer in LookupTree to replace creator as owner and replicate change to
         *                      backup superpeers
         *          iv)  This is the responsible superpeer: Remove peer (locally)
         *          In ZooKeeperBootComponent:
         *          i)   This is the responsible superpeer: Remove peer from "/nodes/peers"
         *          ii)  This is the responsible superpeer: Add ChunkID of failed peer to "/nodes/free" if node was not registered in file at startup
         *          iii) This is the responsible superpeer: If peer was not registered in file at startup, remove its ChunkID from "/nodes/new"
         *      c) Failed node was a terminal:
         *          In LookupComponent:
         *          i)   This is the responsible superpeer: Remove terminal (locally; a terminal is handled like a peer here)
         *          In ZooKeeperBootComponent:
         *          i)   This is the responsible superpeer: Remove peer from "/nodes/terminals"
         *          ii)  This is the responsible superpeer: Add ChunkID of failed terminal to "/nodes/free" if node was not registered in file at startup
         *          iii) This is the responsible superpeer: If terminal was not registered in file at startup (default), remove its ChunkID from "/nodes/new"
         *
         *
         *  2) This is a peer:
         *      a) Failed node was a superpeer: Nothing
         *      b) Failed node was a peer:
         *          Fire NodeFailureEvent:
         *              In BackupComponent (if backup is active):
         *              i)   Iterate over all backup ranges
         *                      For every hit:
         *                      - Determine a new backup peer
         *                      - Replace it in local backup range
         *                      In ChunkBackupComponent:
         *                      - Replicate every single chunk of this backup range to the new backup peer
         *                      In LookupComponent
         *                      - Replace the failed backup peer by the new determined backup peer on the responsible superpeer
         *              In LookupComponent (if caches are enabled):
         *              i) Invalidate all cached lookup ranges in CacheTree that store the failed peer as current location
         *              In PeerLockService:
         *              i) Unlock all remote locks (for chunks stored on this peer) that are held by the failed peer
         *      c) Failed node was a terminal:
         *          Fire NodeFailureEvent:
         *              In PeerLockService:
         *              i) Unlock all remote locks (for chunks stored on this peer) that are held by the failed terminal
         *
         *
         *  3) This is a terminal:
         *      a) Failed node was a superpeer: Nothing
         *      b) Failed node was a peer:
         *          Fire NodeFailureEvent:
         *              In LookupComponent (if caches are enabled):
         *              i) Invalidate all cached lookup ranges in CacheTree that store the failed peer as current location
         *      c) Failed node was a terminal: Nothing
         */

        if (ownRole == NodeRole.SUPERPEER) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("********** ********** Node Failure ********** **********");
            // #endif /* LOGGER >= DEBUG */

            // Restore superpeer overlay, cleanup ZooKeeper and/or initiate recovery
            responsible = m_lookup.superpeersNodeFailureHandling(p_nodeID, roleOfFailedNode);

            if (responsible) {
                // Failed node was either the predecessor superpeer or a peer/terminal this superpeer is responsible for

                // #if LOGGER >= DEBUG
                LOGGER.debug("Failed node was a %s, NodeID: 0x%X", roleOfFailedNode, p_nodeID);
                // #endif /* LOGGER >= DEBUG */
            } else {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Not responsible for failed node, NodeID: 0x%X", p_nodeID);
                // #endif /* LOGGER >= DEBUG */
            }
        } else {
            // This is a peer or terminal
            if (roleOfFailedNode == NodeRole.PEER || roleOfFailedNode == NodeRole.TERMINAL) {
                // Notify other components/services (BackupComponent, LookupComponent, PeerLockService)
                m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_nodeID, roleOfFailedNode));
            }
        }
    }

    /**
     * Handles an incoming FailureRequest
     *
     * @param p_request
     *     the FailureRequest
     */
    private void incomingFailureRequest(final FailureRequest p_request) {
        // Outsource failure handling to another thread to avoid blocking a message handler
        Runnable task = () -> {
            short nodeID = p_request.getFailedNode();

            m_failureLock.lock();
            if (m_nodeStatus[nodeID & 0xFFFF] < 5) {
                m_nodeStatus[nodeID & 0xFFFF] = 5;
                m_failureLock.unlock();

                failureHandling(nodeID);

                m_failureLock.lock();
                m_nodeStatus[nodeID & 0xFFFF] = 0;
                m_failureLock.unlock();
            } else {
                m_failureLock.unlock();
            }
        };
        new Thread(task).start();

        try {
            m_network.sendMessage(new FailureResponse(p_request));
        } catch (final NetworkException ignore) {

        }
    }

}
