package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSLevelFinishedMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Created by nothaas on 9/1/16.
 */
public class SyncBFSFinished implements NetworkHandler.MessageReceiver {
	private short[] m_nodeIDs;
	private short m_ownNodeID;
	private NetworkService m_networkService;

	private volatile boolean m_signalAbortExecution;

	private AtomicLong m_sentVertexMsgCountLocal = new AtomicLong(0);
	private AtomicLong m_recvVertexMsgCountLocal = new AtomicLong(0);

	private Lock m_msgCountGlobalLock = new ReentrantLock(false);
	private AtomicInteger m_bfsSlavesLevelFinishedCounter = new AtomicInteger(0);
	private long m_sentVertexMsgCountGlobal;
	private long m_recvVertexMsgCountGlobal;

	public SyncBFSFinished(final short[] p_nodeIDs, final short p_ownNodeID, final NetworkService p_networkService) {
		m_nodeIDs = p_nodeIDs;
		m_ownNodeID = p_ownNodeID;

		m_signalAbortExecution = false;

		// TODO register message handler
		// TODO register message

		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE,
				BFSLevelFinishedMessage.class);

		m_networkService.registerReceiver(BFSLevelFinishedMessage.class, this);
	}

	public void signalAbortExecution() {
		m_signalAbortExecution = true;
	}

	public boolean execute() {
		while (true) {
			System.out.println("!!!!!!!!!!!!!!!!!!");
			// inform all other slaves we are done

			long localSentMsgCnt = m_sentVertexMsgCountLocal.get();
			long localRecvMsgCnt = m_recvVertexMsgCountLocal.get();

			System.out.println(">>>>> local: " + localSentMsgCnt + " | " + localRecvMsgCnt);

			for (int i = 0; i < m_nodeIDs.length; i++) {
				if (i != m_ownNodeID) {
					BFSLevelFinishedMessage msg = new BFSLevelFinishedMessage(m_nodeIDs[i],
							localSentMsgCnt, localRecvMsgCnt);
					System.out.println(
							"<<<<< BFSLevelFinishedMessage " + NodeID.toHexString(m_nodeIDs[i]));
					NetworkErrorCodes err = m_networkService.sendMessage(msg);
					System.out.println(
							"<<<<<??? BFSLevelFinishedMessage " + NodeID.toHexString(m_nodeIDs[i]));
					if (err != NetworkErrorCodes.SUCCESS) {
						// TODO
						//						// #if LOGGER >= ERROR
						//						m_loggerService.error(getClass(),
						//								"Sending level finished message to " + NodeID.toHexString(m_nodeIDs[i])
						//										+ " failed: " + err);
						//						// #endif /* LOGGER >= ERROR */
						//						// abort execution and have the master send a kill signal
						//						// for this task to all other slaves
						//						sendSignalToMaster(Signal.SIGNAL_ABORT);
						//						return;
						return false;
					}
				}
			}

			// busy wait until everyone is done
			// TODO doc this properly, correct termination is very tricky especially avoiding killing messages in transit
			System.out.println("!!!!! " + m_nodeIDs.length);
			while (true) {
				m_msgCountGlobalLock.lock();
				int tmp = m_bfsSlavesLevelFinishedCounter.get();
				if (tmp >= m_nodeIDs.length - 1
						&& m_bfsSlavesLevelFinishedCounter
						.compareAndSet(tmp, tmp - (m_nodeIDs.length - 1))) {
					break;
				}
				m_msgCountGlobalLock.unlock();

				try {
					Thread.sleep(2);
				} catch (final InterruptedException ignored) {
				}

				if (m_signalAbortExecution) {
					return false;
				}
			}

			System.out.println("{{{{{{{{{{{{{");

			// check if our total sent and received vertex messages are equal i.e.
			// all sent messages were received and processed
			// repeat this sync steps of some messages are still in transit or
			// not fully processed, yet

			// don't forget to add the counters from our local instance
			m_sentVertexMsgCountGlobal += localSentMsgCnt;
			m_recvVertexMsgCountGlobal += localRecvMsgCnt;

			if (m_sentVertexMsgCountGlobal != m_recvVertexMsgCountGlobal) {
				System.out.println(
						"@@@@ " + m_sentVertexMsgCountGlobal + " != " + m_recvVertexMsgCountGlobal);
				m_sentVertexMsgCountGlobal = 0;
				m_recvVertexMsgCountGlobal = 0;
				m_msgCountGlobalLock.unlock();
			} else {
				System.out.println(
						"@@@@ " + m_sentVertexMsgCountGlobal + " == " + m_recvVertexMsgCountGlobal);
				m_sentVertexMsgCountGlobal = 0;
				m_recvVertexMsgCountGlobal = 0;
				m_sentVertexMsgCountLocal.set(0);
				m_recvVertexMsgCountLocal.set(0);
				m_msgCountGlobalLock.unlock();
				break;
			}
		}

		return true;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == BFSMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE:
						onIncomingBFSLevelFinishedMessage(
								(BFSLevelFinishedMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	private void onIncomingBFSLevelFinishedMessage(final BFSLevelFinishedMessage p_message) {
		System.out.println(">>>> onIncomingBFSLevelFinishedMessage " + NodeID.toHexString(p_message.getSource()));

		// wait for main thread to process
		while (m_bfsSlavesLevelFinishedCounter.get() >= m_nodeIDs.length - 1) {
			try {
				Thread.sleep(2);
			} catch (final InterruptedException ignored) {
			}
		}
		m_msgCountGlobalLock.lock();
		System.out.println(
				"???? from " + NodeID.toHexString(p_message.getSource()) + ": " + p_message.getSentMessageCount()
						+ " | " + p_message.getReceivedMessageCount());
		m_recvVertexMsgCountGlobal += p_message.getReceivedMessageCount();
		m_sentVertexMsgCountGlobal += p_message.getSentMessageCount();
		m_msgCountGlobalLock.unlock();

		m_bfsSlavesLevelFinishedCounter.incrementAndGet();
	}
}
