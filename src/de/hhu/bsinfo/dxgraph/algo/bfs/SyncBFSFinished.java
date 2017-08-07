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

package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSLevelFinishedMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;

/**
 * Created by nothaas on 9/1/16.
 */
public class SyncBFSFinished implements MessageReceiver {
    private short[] m_nodeIDs;
    private short m_ownNodeID;
    private NetworkService m_networkService;

    private volatile boolean m_signalAbortExecution;

    // TODO i think this can be non volatile without breaking anything -> test
    private volatile int m_token;

    private AtomicLong m_sentVertexMsgCountLocal = new AtomicLong(0);
    private AtomicLong m_recvVertexMsgCountLocal = new AtomicLong(0);

    private Semaphore[] m_finishedCounterLock = new Semaphore[] {new Semaphore(1, false), new Semaphore(1, false)};
    private AtomicInteger[] m_bfsSlavesLevelFinishedCounter = new AtomicInteger[] {new AtomicInteger(0), new AtomicInteger(0)};
    private volatile long[] m_sentVertexMsgCountGlobal = new long[2];
    private volatile long[] m_recvVertexMsgCountGlobal = new long[2];

    SyncBFSFinished(final short[] p_nodeIDs, final short p_ownNodeID, final NetworkService p_networkService) {
        m_nodeIDs = p_nodeIDs;
        m_ownNodeID = p_ownNodeID;
        m_networkService = p_networkService;

        m_signalAbortExecution = false;

        m_networkService
                .registerMessageType(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE, BFSLevelFinishedMessage.class);

        m_networkService.registerReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE, this);
    }

    void incrementSentVertexMsgCountLocal() {
        m_sentVertexMsgCountLocal.incrementAndGet();
    }

    void incrementReceivedVertexMsgCountLocal() {
        m_recvVertexMsgCountLocal.incrementAndGet();
    }

    long getSentVertexMsgCountLocal() {
        return m_sentVertexMsgCountLocal.get();
    }

    long getReceivedVertexMsgCountLocal() {
        return m_recvVertexMsgCountLocal.get();
    }

    int getTokenValue() {
        return m_token;
    }

    long getSentVertexMsgCountGlobal() {
        return m_sentVertexMsgCountGlobal[m_token];
    }

    long getReceivedVertexMsgCountGlobal() {
        return m_recvVertexMsgCountGlobal[m_token];
    }

    int getBFSSlavesLevelFinishedCounter() {
        return m_bfsSlavesLevelFinishedCounter[m_token].get();
    }

    void signalAbortExecution() {
        m_signalAbortExecution = true;
    }

    boolean execute() {
        // use double buffering for certain data
        // if we happen to have multiple iterations due to multiple repeats necessary,
        // we have to make sure that finished messages from nodes that can already determine
        // that another iteration i is necessary do not interfere with nodes that still have to
        // figure that out in iteration i - 1 i.e. messages of the next evaluation phase i
        // overtake messages of the phase i - 1 when processing them in the message handler
        m_token = 0;

        while (true) {
            // inform all other slaves we are done, this might need multiple tries
            // because some vertex data messages (on the bottom up approach, only)
            // arrive later because they are sent AFTER the sync messages are sent
            // this cannot be determined, thus we need to count sent and received
            // messages of all nodes and have successfully synchronized all nodes if
            // 1. all nodes have responded to the current sync phase number (token)
            // 2. the sum of all sent and received messages of all nodes is equal
            // the first step is necessary to continue with the second one.
            // if the first one "fails" we loop and wait because one node is not responding i.e. not done
            // if the second step fails, we know that at least one node has still vertex data messages
            // either in transit, unprocessed or currently being processed.
            // thus, we have to go back to the first step and repeat everything hoping
            // that all nodes are done on the next try

            long localSentMsgCnt = m_sentVertexMsgCountLocal.get();
            long localRecvMsgCnt = m_recvVertexMsgCountLocal.get();

            for (short nodeID : m_nodeIDs) {
                if (nodeID != m_ownNodeID) {
                    BFSLevelFinishedMessage msg = new BFSLevelFinishedMessage(nodeID, m_token, localSentMsgCnt, localRecvMsgCnt);

                    try {
                        m_networkService.sendMessage(msg);
                    } catch (final NetworkException e) {
                        return false;
                    }
                }
            }

            // busy wait until everyone is done
            while (true) {
                if (m_bfsSlavesLevelFinishedCounter[m_token].compareAndSet(m_nodeIDs.length - 1, 0)) {
                    break;
                }

                try {
                    Thread.sleep(2);
                } catch (final InterruptedException ignored) {
                }

                if (m_signalAbortExecution) {
                    return false;
                }
            }

            // check if our total sent and received vertex messages are equal i.e.
            // all sent messages were received and processed
            // repeat this sync steps of some messages are still in transit or
            // not fully processed, yet

            // don't forget to add the counters from our local instance
            m_sentVertexMsgCountGlobal[m_token] += localSentMsgCnt;
            m_recvVertexMsgCountGlobal[m_token] += localRecvMsgCnt;

            if (m_sentVertexMsgCountGlobal[m_token] != m_recvVertexMsgCountGlobal[m_token]) {
                m_sentVertexMsgCountGlobal[m_token] = 0;
                m_recvVertexMsgCountGlobal[m_token] = 0;
                m_finishedCounterLock[m_token].release();
                m_token = (m_token + 1) % 2;
            } else {
                m_sentVertexMsgCountGlobal[m_token] = 0;
                m_recvVertexMsgCountGlobal[m_token] = 0;
                m_sentVertexMsgCountLocal.set(0);
                m_recvVertexMsgCountLocal.set(0);
                m_finishedCounterLock[m_token].release();
                break;
            }
        }

        return true;
    }

    void cleanup() {
        m_networkService.unregisterReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE, this);
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXGraphMessageTypes.BFS_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE:
                        onIncomingBFSLevelFinishedMessage((BFSLevelFinishedMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void onIncomingBFSLevelFinishedMessage(final BFSLevelFinishedMessage p_message) {

        try {
            m_finishedCounterLock[p_message.getToken()].acquire();
        } catch (final InterruptedException ignored) {
        }

        m_recvVertexMsgCountGlobal[p_message.getToken()] += p_message.getReceivedMessageCount();
        m_sentVertexMsgCountGlobal[p_message.getToken()] += p_message.getSentMessageCount();

        // don't unlock on last message main thread is waiting for,
        // main thread is unlocking
        if (m_bfsSlavesLevelFinishedCounter[p_message.getToken()].incrementAndGet() != m_nodeIDs.length - 1) {
            m_finishedCounterLock[p_message.getToken()].release();
        }
    }
}
