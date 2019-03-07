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

package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorResponse;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewPredecessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewSuccessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendBackupsMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendSuperpeersMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Stabilizes superpeer overlay
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.06.2013
 */
class SuperpeerStabilizationThread extends Thread implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(
            SuperpeerStabilizationThread.class);

    // Attributes
    private NetworkComponent m_network;

    private OverlaySuperpeer m_superpeer;

    private int m_initialNumberOfSuperpeers;
    private ArrayList<Short> m_otherSuperpeers;
    private ReentrantReadWriteLock m_overlayLock;

    private short m_nodeID;
    private int m_sleepInterval;
    private int m_next;
    private boolean m_shutdown;

    private String m_overlayFigure;

    // Constructors

    /**
     * Creates an instance of Worker
     *
     * @param p_superpeer
     *         the overlay superpeer
     * @param p_nodeID
     *         the own NodeID
     * @param p_overlayLock
     *         the overlay lock
     * @param p_initialNumberOfSuperpeers
     *         the number of expected superpeers
     * @param p_superpeers
     *         all other superpeers
     * @param p_sleepInterval
     *         the ping interval in ms
     * @param p_network
     *         the network component
     */
    SuperpeerStabilizationThread(final OverlaySuperpeer p_superpeer, final short p_nodeID,
            final ReentrantReadWriteLock p_overlayLock, final int p_initialNumberOfSuperpeers,
            final ArrayList<Short> p_superpeers, final int p_sleepInterval, final NetworkComponent p_network) {
        super("stabilization");
        m_superpeer = p_superpeer;

        m_network = p_network;

        m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;
        m_otherSuperpeers = p_superpeers;
        m_overlayLock = p_overlayLock;

        m_nodeID = p_nodeID;
        m_sleepInterval = p_sleepInterval;
        m_next = 0;

        registerNetworkMessageListener();
    }

    /**
     * When an object implementing interface {@code Runnable} is used
     * to create a thread, starting the thread causes the object's {@code run} method to be called in that
     * separately executing
     * thread.
     * The general contract of the method {@code run} is that it may take any action whatsoever.
     *
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        while (!m_shutdown) {
            try {
                Thread.sleep(m_sleepInterval);
            } catch (final InterruptedException ignored) {
                m_shutdown = true;
                break;
            }

            performStabilization();

            for (int i = 0; i < m_initialNumberOfSuperpeers / 300 || i < 1; i++) {
                fixSuperpeers();
            }

            if (!m_otherSuperpeers.isEmpty()) {
                backupMaintenance();

                m_overlayLock.writeLock().lock();
                m_superpeer.takeOverPeers(m_nodeID);
                m_overlayLock.writeLock().unlock();
            }

            pingPeers();

            printOverlay();
        }
    }

    /**
     * Handles an incoming Message
     *
     * @param p_message
     *         the Message
     */
    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE:
                        incomingSendBackupsMessage((SendBackupsMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST:
                        incomingAskAboutBackupsRequest((AskAboutBackupsRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST:
                        incomingAskAboutSuccessorRequest((AskAboutSuccessorRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE:
                        incomingNotifyAboutNewPredecessorMessage((NotifyAboutNewPredecessorMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE:
                        incomingNotifyAboutNewSuccessorMessage((NotifyAboutNewSuccessorMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE:
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Shutdown
     */
    protected void shutdown() {
        m_shutdown = true;
    }

    /**
     * Performs stabilization protocol
     *
     * @note without disappearing superpeers this method does not do anything important;
     * All the setup is done with joining
     */
    private void performStabilization() {
        m_overlayLock.readLock().lock();
        while (m_superpeer.getPredecessor() != NodeID.INVALID_ID && m_nodeID != m_superpeer.getPredecessor()) {

            LOGGER.trace("Performing stabilization by sending NodeID to predecessor=0x%X",
                    m_superpeer.getPredecessor());

            try {
                m_network.sendMessage(new NotifyAboutNewSuccessorMessage(m_superpeer.getPredecessor(), m_nodeID));
            } catch (final NetworkException e) {
                // Predecessor is not available anymore, repeat it. New predecessor will be determined by failure
                // handling, but lock must be released first.
                m_overlayLock.readLock().unlock();
                Thread.yield();
                m_overlayLock.readLock().lock();
                continue;
            }

            break;
        }

        while (m_superpeer.getSuccessor() != NodeID.INVALID_ID && m_nodeID != m_superpeer.getSuccessor()) {
            LOGGER.trace("Performing stabilization by sending NodeID to successor=0x%X", m_superpeer.getSuccessor());

            try {
                m_network.sendMessage(new NotifyAboutNewPredecessorMessage(m_superpeer.getSuccessor(), m_nodeID));
            } catch (final NetworkException e) {
                // Predecessor is not available anymore, repeat it. New predecessor will be determined by failure
                // handling, but lock must be released first.
                m_overlayLock.readLock().unlock();
                Thread.yield();
                m_overlayLock.readLock().lock();
                continue;
            }

            break;
        }
        m_overlayLock.readLock().unlock();
    }

    /**
     * Fixes the superpeer array
     */
    private void fixSuperpeers() {
        short contactSuperpeer;
        short possibleSuccessor;
        short hisSuccessor;

        AskAboutSuccessorRequest request;
        AskAboutSuccessorResponse response;

        m_overlayLock.readLock().lock();
        if (m_otherSuperpeers.size() > 1) {
            if (m_next + 1 < m_otherSuperpeers.size()) {
                contactSuperpeer = m_otherSuperpeers.get(m_next);
                possibleSuccessor = m_otherSuperpeers.get(m_next + 1);
            } else if (m_next + 1 == m_otherSuperpeers.size()) {
                contactSuperpeer = m_otherSuperpeers.get(m_next);
                possibleSuccessor = m_otherSuperpeers.get(0);
            } else {
                m_next = 0;
                m_overlayLock.readLock().unlock();
                fixSuperpeers();
                return;
            }

            if (contactSuperpeer == m_superpeer.getPredecessor()) {
                m_next++;
                m_overlayLock.readLock().unlock();
                fixSuperpeers();
                return;
            }
            m_overlayLock.readLock().unlock();

            m_next++;

            LOGGER.trace("Asking 0x%X about his successor to fix overlay", contactSuperpeer);

            request = new AskAboutSuccessorRequest(contactSuperpeer);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // Superpeer is not available anymore, remove from superpeer array and try next superpeer
                m_next--;
                fixSuperpeers();
                return;
            }

            response = request.getResponse(AskAboutSuccessorResponse.class);

            hisSuccessor = response.getSuccessor();

            if (hisSuccessor != possibleSuccessor && hisSuccessor != NodeID.INVALID_ID) {
                m_overlayLock.writeLock().lock();
                OverlayHelper.insertSuperpeer(hisSuccessor, m_otherSuperpeers);
                m_overlayLock.writeLock().unlock();
            }
        } else {
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Pings all peers and sends current superpeer overlay
     */
    private void pingPeers() {
        short peer;
        int i = 0;

        m_overlayLock.readLock().lock();
        final ArrayList<Short> peers = m_superpeer.getPeers();
        m_overlayLock.readLock().unlock();

        if (peers != null && !peers.isEmpty()) {
            while (true) {
                if (i < peers.size()) {
                    peer = peers.get(i++);
                } else {
                    break;
                }

                LOGGER.trace("Pinging 0x%X for heartbeat protocol", peer);

                try {
                    m_network.sendMessage(new SendSuperpeersMessage(peer, m_otherSuperpeers));
                } catch (final NetworkException e) {
                    // Peer is not available anymore, will be removed from peer array by failure handling
                }
            }
        }
    }

    /**
     * Maintain backup replication
     */
    private void backupMaintenance() {
        short[] responsibleArea;

        m_overlayLock.readLock().lock();
        responsibleArea = OverlayHelper.getResponsibleArea(m_nodeID, m_superpeer.getPredecessor(), m_otherSuperpeers);
        m_overlayLock.readLock().unlock();

        LOGGER.trace("Responsible backup area: 0x%X, 0x%X", responsibleArea[0], responsibleArea[1]);

        gatherBackups(responsibleArea);

        m_overlayLock.writeLock().lock();
        m_superpeer.deleteUnnecessaryBackups(responsibleArea);
        m_overlayLock.writeLock().unlock();
    }

    /**
     * Gather all missing metadata in the responsible area
     *
     * @param p_responsibleArea
     *         the responsible area
     */
    private void gatherBackups(final short[] p_responsibleArea) {
        short currentSuperpeer;
        short oldSuperpeer;
        ArrayList<Short> peers;
        int numberOfNameserviceEntries;
        int numberOfStorages;
        int numberOfBarriers;
        short[] currentResponsibleArea;

        AskAboutBackupsRequest request;
        AskAboutBackupsResponse response;

        m_overlayLock.readLock().lock();
        if (!m_otherSuperpeers.isEmpty()) {
            if (m_otherSuperpeers.size() <= 3) {
                oldSuperpeer = m_nodeID;
                currentSuperpeer = m_superpeer.getSuccessor();
            } else {
                oldSuperpeer = p_responsibleArea[0];
                currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (p_responsibleArea[0] + 1),
                        m_otherSuperpeers);
            }
            while (currentSuperpeer != NodeID.INVALID_ID) {
                peers = m_superpeer.getPeersInResponsibleArea(oldSuperpeer, currentSuperpeer);

                LOGGER.trace("Gathering backups by requesting all backups in responsible area from 0x%X",
                        currentSuperpeer);

                currentResponsibleArea = new short[] {oldSuperpeer, currentSuperpeer};
                numberOfNameserviceEntries = m_superpeer.getNumberOfNameserviceEntries(currentResponsibleArea);
                numberOfStorages = m_superpeer.getNumberOfStorages(currentResponsibleArea);
                numberOfBarriers = m_superpeer.getNumberOfBarriers(currentResponsibleArea);
                request = new AskAboutBackupsRequest(currentSuperpeer, peers, numberOfNameserviceEntries,
                        numberOfStorages, numberOfBarriers);
                m_overlayLock.readLock().unlock();

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // CurrentSuperpeer is not available anymore, will be removed from superpeer array by failure
                    // handling
                    m_overlayLock.readLock().lock();
                    currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (oldSuperpeer + 1),
                            m_otherSuperpeers);
                    peers.clear();
                    continue;
                }

                response = request.getResponse(AskAboutBackupsResponse.class);

                m_overlayLock.writeLock().lock();
                m_superpeer.storeIncomingBackups(response.getMissingMetadata());
                // Lock downgrade
                m_overlayLock.readLock().lock();
                m_overlayLock.writeLock().unlock();

                if (currentSuperpeer == m_superpeer.getPredecessor() || !OverlayHelper.isSuperpeerInRange(
                        currentSuperpeer, p_responsibleArea[0], m_nodeID)) {
                    // Second case is for predecessor failure
                    break;
                }

                peers.clear();

                oldSuperpeer = currentSuperpeer;
                currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (currentSuperpeer + 1),
                        m_otherSuperpeers);
            }
        }
        m_overlayLock.readLock().unlock();
    }

    /**
     * Prints the overlay if something has changed since last call
     */
    private void printOverlay() {
        boolean printed = false;
        short superpeer;
        short peer;
        StringBuilder superpeersFigure = new StringBuilder("Superpeers: ");
        StringBuilder peersFigure = new StringBuilder("Peers: ");

        m_overlayLock.readLock().lock();
        for (int i = 0; i < m_otherSuperpeers.size(); i++) {
            superpeer = m_otherSuperpeers.get(i);
            if (!printed && superpeer > m_nodeID) {
                superpeersFigure.append(" \'").append(NodeID.toHexString(m_nodeID)).append('\'');
                printed = true;
            }
            superpeersFigure.append(' ').append(NodeID.toHexString(superpeer));
        }
        if (!printed) {
            superpeersFigure.append(" \'").append(NodeID.toHexString(m_nodeID)).append('\'');
        }

        final ArrayList<Short> peers = m_superpeer.getPeers();
        if (peers != null && !peers.isEmpty()) {
            for (int i = 0; i < peers.size(); i++) {
                peer = peers.get(i);
                peersFigure.append(' ').append(NodeID.toHexString(peer));
            }
        }
        m_overlayLock.readLock().unlock();

        if (!(superpeersFigure + peersFigure.toString()).equals(m_overlayFigure)) {

            LOGGER.debug(superpeersFigure.toString());
            if (!"Peers: ".equals(peersFigure.toString())) {
                LOGGER.debug(peersFigure.toString());
            }

        }
        m_overlayFigure = superpeersFigure + peersFigure.toString();
    }

    /**
     * Handles an incoming SendBackupsMessage
     *
     * @param p_sendBackupsMessage
     *         the SendBackupsMessage
     */
    private void incomingSendBackupsMessage(final SendBackupsMessage p_sendBackupsMessage) {

        LOGGER.trace("Got Message: SEND_BACKUPS_MESSAGE from 0x%X", p_sendBackupsMessage.getSource());

        m_overlayLock.writeLock().lock();
        m_superpeer.storeIncomingBackups(p_sendBackupsMessage.getMetadata());
        m_overlayLock.writeLock().unlock();
    }

    /**
     * Handles an incoming AskAboutBackupsRequest
     *
     * @param p_askAboutBackupsRequest
     *         the AskAboutBackupsRequest
     */
    private void incomingAskAboutBackupsRequest(final AskAboutBackupsRequest p_askAboutBackupsRequest) {
        byte[] missingMetadata;

        LOGGER.trace("Got request: ASK_ABOUT_SUCCESSOR_REQUEST from 0x%X", p_askAboutBackupsRequest.getSource());

        m_overlayLock.readLock().lock();
        missingMetadata = m_superpeer.compareAndReturnBackups(p_askAboutBackupsRequest.getPeers(),
                p_askAboutBackupsRequest.getNumberOfNameserviceEntries(),
                p_askAboutBackupsRequest.getNumberOfStorages(), p_askAboutBackupsRequest.getNumberOfBarriers());
        m_overlayLock.readLock().unlock();

        try {
            m_network.sendMessage(new AskAboutBackupsResponse(p_askAboutBackupsRequest, missingMetadata));
        } catch (final NetworkException e) {
            // Requesting superpeer is not available anymore, ignore request. Superpeer will be removed by failure
            // handling.
        }
    }

    /**
     * Handles an incoming AskAboutSuccessorRequest
     *
     * @param p_askAboutSuccessorRequest
     *         the AskAboutSuccessorRequest
     */
    private void incomingAskAboutSuccessorRequest(final AskAboutSuccessorRequest p_askAboutSuccessorRequest) {
        short successor;

        LOGGER.trace("Got request: ASK_ABOUT_SUCCESSOR_REQUEST from 0x%X", p_askAboutSuccessorRequest.getSource());

        m_overlayLock.readLock().lock();
        successor = m_superpeer.getSuccessor();
        m_overlayLock.readLock().unlock();
        try {
            m_network.sendMessage(new AskAboutSuccessorResponse(p_askAboutSuccessorRequest, successor));
        } catch (final NetworkException e) {
            // Requesting superpeer is not available anymore, ignore request. Superpeer will be removed by failure
            // handling.
        }
    }

    /**
     * Handles an incoming NotifyAboutNewPredecessorMessage
     *
     * @param p_notifyAboutNewPredecessorMessage
     *         the NotifyAboutNewPredecessorMessage
     */
    private void incomingNotifyAboutNewPredecessorMessage(
            final NotifyAboutNewPredecessorMessage p_notifyAboutNewPredecessorMessage) {
        short possiblePredecessor;

        LOGGER.trace("Got Message: NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE from 0x%X",
                p_notifyAboutNewPredecessorMessage.getSource());

        possiblePredecessor = p_notifyAboutNewPredecessorMessage.getNewPredecessor();
        m_overlayLock.writeLock().lock();
        if (m_superpeer.getPredecessor() != possiblePredecessor) {
            if (OverlayHelper.isSuperpeerInRange(possiblePredecessor, m_superpeer.getPredecessor(), m_nodeID)) {
                m_superpeer.setPredecessor(possiblePredecessor);
            }
        }
        m_overlayLock.writeLock().unlock();
    }

    /**
     * Handles an incoming NotifyAboutNewSuccessorMessage
     *
     * @param p_notifyAboutNewSuccessorMessage
     *         the NotifyAboutNewSuccessorMessage
     */
    private void incomingNotifyAboutNewSuccessorMessage(
            final NotifyAboutNewSuccessorMessage p_notifyAboutNewSuccessorMessage) {
        short possibleSuccessor;

        LOGGER.trace("Got Message: NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE from 0x%X",
                p_notifyAboutNewSuccessorMessage.getSource());

        possibleSuccessor = p_notifyAboutNewSuccessorMessage.getNewSuccessor();
        m_overlayLock.writeLock().lock();
        if (m_superpeer.getSuccessor() != possibleSuccessor) {
            if (OverlayHelper.isSuperpeerInRange(possibleSuccessor, m_nodeID, m_superpeer.getSuccessor())) {
                m_superpeer.setSuccessor(possibleSuccessor);
            }
        }
        m_overlayLock.writeLock().unlock();
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, this);
    }
}
