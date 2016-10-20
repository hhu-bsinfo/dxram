
package de.hhu.bsinfo.dxram.boot;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxram.boot.messages.BootMessages;
import de.hhu.bsinfo.dxram.boot.messages.RebootMessage;
import de.hhu.bsinfo.dxram.boot.messages.ShutdownMessage;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Service providing information about the bootstrapping process like
 * node ids, node roles, addresses etc.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class BootService extends AbstractDXRAMService implements MessageReceiver {

	// dependent components
	private AbstractBootComponent m_boot;
	private NetworkComponent m_network;
	private LoggerComponent m_logger;

	/**
	 * Constructor
	 */
	public BootService() {
		super("boot");
	}

	/**
	 * Check if a specific node is online.
	 *
	 * @param p_nodeID Node to check.
	 * @return True if online, false offline.
	 */
	public boolean isNodeOnline(final short p_nodeID) {
		return m_boot.isNodeOnline(p_nodeID);
	}

	/**
	 * Get the ID of the node, you are currently running on.
	 *
	 * @return NodeID.
	 */
	public short getNodeID() {
		return m_boot.getNodeID();
	}

	/**
	 * Get IDs of all available (online) nodes including own.
	 *
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getOnlineNodeIDs() {
		return m_boot.getIDsOfOnlineNodes();
	}

	/**
	 * Get the node role of the current node.
	 *
	 * @return Node role of current node.
	 */
	public NodeRole getNodeRole() {
		return m_boot.getNodeRole();
	}

	/**
	 * Get the role of another nodeID.
	 *
	 * @param p_nodeID Node id to get the role of.
	 * @return Role of other nodeID or null if node does not exist.
	 */
	public NodeRole getNodeRole(final short p_nodeID) {
		return m_boot.getNodeRole(p_nodeID);
	}

	/**
	 * Get the IP and port of another node.
	 *
	 * @param p_nodeID Node ID of the node.
	 * @return IP and port of the specified node or an invalid address if not available.
	 */
	public InetSocketAddress getNodeAddress(final short p_nodeID) {
		return m_boot.getNodeAddress(p_nodeID);
	}

	/**
	 * Get IDs of all available (online) peer nodes exception our own.
	 *
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getOnlineSuperpeerNodeIDs() {
		return m_boot.getIDsOfOnlineSuperpeers();
	}

	/**
	 * Get IDs of all available (online) peer nodes exception our own.
	 *
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getOnlinePeerNodeIDs() {
		return m_boot.getIDsOfOnlinePeers();
	}

	/**
	 * Check if a node is available/exists.
	 *
	 * @param p_nodeID Node ID to check.
	 * @return True if available, false otherwise.
	 */
	public boolean nodeAvailable(final short p_nodeID) {
		return m_boot.nodeAvailable(p_nodeID);
	}

	/**
	 * Shutdown a single node or all nodes.
	 *
	 * @param p_nodeID       Node id to shut down or -1/0xFFFF to shut down all nodes.
	 * @param p_hardShutdown If true this will kill the process instead of shutting down DXRAM properly.
	 * @return True if successful, false on failure.
	 */
	public boolean shutdownNode(final short p_nodeID, final boolean p_hardShutdown) {
		if (p_nodeID == m_boot.getNodeID()) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Shutting down ourselves is not possible like this.");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (p_nodeID == -1) {
			List<Short> nodeIds = m_boot.getIDsOfOnlineNodes();

			// shutdown peers first
			for (Short nodeId : nodeIds) {
				if (nodeId != m_boot.getNodeID() && m_boot.getNodeRole(nodeId) == NodeRole.PEER) {
					ShutdownMessage message = new ShutdownMessage(nodeId, p_hardShutdown);
					NetworkErrorCodes err = m_network.sendMessage(message);
					if (err != NetworkErrorCodes.SUCCESS) {
						// #if LOGGER >= ERROR
						m_logger.error(getClass(),
								"Shutting down node " + NodeID.toHexString(nodeId) + " failed: " + err);
						// #endif /* LOGGER >= ERROR */
						return false;
					}
				}
			}

			// some delay so peers still have their superpeers when shutting down
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// then superpeers
			for (Short nodeId : nodeIds) {
				if (nodeId != m_boot.getNodeID() && m_boot.getNodeRole(nodeId) == NodeRole.SUPERPEER) {
					ShutdownMessage message = new ShutdownMessage(nodeId, p_hardShutdown);
					NetworkErrorCodes err = m_network.sendMessage(message);
					if (err != NetworkErrorCodes.SUCCESS) {
						// #if LOGGER >= ERROR
						m_logger.error(getClass(),
								"Shutting down node " + NodeID.toHexString(nodeId) + " failed: " + err);
						// #endif /* LOGGER >= ERROR */
						return false;
					}
				}
			}

			// and ourselves
			shutdown(p_hardShutdown);

		} else {
			ShutdownMessage message = new ShutdownMessage(p_nodeID, p_hardShutdown);
			NetworkErrorCodes err = m_network.sendMessage(message);
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Shutting down node " + NodeID.toHexString(p_nodeID) + " failed: " + err);
				// #endif /* LOGGER >= ERROR */
				return false;
			}

			// #if LOGGER >= INFO
			m_logger.info(getClass(), "Sent remote shutdown to node " + NodeID.toHexString(p_nodeID));
			// #endif /* LOGGER >= INFO */
		}

		return true;
	}

	/**
	 * (Soft) reboot a DXRAM node.
	 *
	 * @param p_nodeID Node to reboot.
	 * @return True if successful, false otherwise.
	 */
	public boolean rebootNode(final short p_nodeID) {
		if (p_nodeID == m_boot.getNodeID()) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Rebooting ourselves is not possible like this.");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		RebootMessage message = new RebootMessage(p_nodeID);
		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Rebooting node " + NodeID.toHexString(p_nodeID) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Sent reboot message to node " + NodeID.toHexString(p_nodeID));
		// #endif /* LOGGER >= INFO */

		return true;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.BOOT_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case BootMessages.SUBTYPE_REBOOT_MESSAGE:
						incomingRebootMessage((RebootMessage) p_message);
						break;
					case BootMessages.SUBTYPE_SHUTDOWN_MESSAGE:
						incomingShutdownMessage((ShutdownMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_boot = getComponent(AbstractBootComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_logger = getComponent(LoggerComponent.class);

		m_network.registerMessageType(DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_REBOOT_MESSAGE,
				RebootMessage.class);
		m_network.registerMessageType(DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE,
				ShutdownMessage.class);

		m_network.register(RebootMessage.class, this);
		m_network.register(ShutdownMessage.class, this);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_boot = null;
		m_logger = null;

		m_network.unregister(RebootMessage.class, this);
		m_network.unregister(ShutdownMessage.class, this);

		m_network = null;

		return true;
	}

	@Override
	protected boolean isEngineAccessor() {
		return true;
	}

	/**
	 * Handler an incoming RebootMessage.
	 *
	 * @param p_message Message to handle.
	 */
	private void incomingRebootMessage(final RebootMessage p_message) {
		DXRAMEngine parentEngine = getParentEngine();
		new Thread() {
			@Override
			public void run() {
				parentEngine.shutdown();
				// wait a moment for the superpeer to detect the failure
				try {
					Thread.sleep(2000);
				} catch (final InterruptedException ignored) {
				}
				parentEngine.init();
			}
		}.start();
	}

	/**
	 * Handler an incoming ShutdownMessage.
	 *
	 * @param p_message Message to handle.
	 */
	private void incomingShutdownMessage(final ShutdownMessage p_message) {
		shutdown(p_message.isHardShutdown());
	}

	/**
	 * Shutdown the current node.
	 *
	 * @param p_hardShutdown True to kill the node without shutting down DXRAM, false for proper DXRAM shutdown.
	 */
	private void shutdown(final boolean p_hardShutdown) {
		if (p_hardShutdown) {
			// suicide
			// note: this might not work correctly on every jvm implementation
			String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
			try {
				Runtime.getRuntime().exec("kill -9 " + pid);
			} catch (final IOException ignored) {

			}

		} else {
			// triggers the registered cleanup handler
			System.exit(0);
		}
	}
}
