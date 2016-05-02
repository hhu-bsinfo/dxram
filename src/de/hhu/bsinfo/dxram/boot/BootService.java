
package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxram.boot.messages.BootMessages;
import de.hhu.bsinfo.dxram.boot.messages.RebootMessage;
import de.hhu.bsinfo.dxram.boot.messages.ShutdownMessage;
import de.hhu.bsinfo.dxram.boot.tcmds.TcmdNodeReboot;
import de.hhu.bsinfo.dxram.boot.tcmds.TcmdNodeShutdown;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Service providing information about the bootstrapping process like
 * node ids, node roles, addresses etc.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class BootService extends AbstractDXRAMService implements MessageReceiver {

	private AbstractBootComponent m_boot;
	private NetworkComponent m_network;
	private LoggerComponent m_logger;
	private TerminalComponent m_terminal;

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
	public List<Short> getIDsOfOnlineNodes() {
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
	public List<Short> getAvailablePeerNodeIDs() {
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

	public boolean shutdownNode(final short p_nodeID, final boolean p_hardShutdown) {
		if (p_nodeID == m_boot.getNodeID()) {
			m_logger.error(getClass(), "Shutting down ourselves is not possible like this.");
			return false;
		}

		ShutdownMessage message = new ShutdownMessage(p_nodeID, p_hardShutdown);
		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Shutting down node " + NodeID.toHexString(p_nodeID) + " failed: " + err);
			return false;
		}

		m_logger.info(getClass(), "Sent remote shutdown to node " + NodeID.toHexString(p_nodeID));
		return true;
	}

	public boolean rebootNode(final short p_nodeID) {
		if (p_nodeID == m_boot.getNodeID()) {
			m_logger.error(getClass(), "Rebooting ourselves is not possible like this.");
			return false;
		}

		RebootMessage message = new RebootMessage(p_nodeID);
		NetworkErrorCodes err = m_network.sendMessage(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Rebooting node " + NodeID.toHexString(p_nodeID) + " failed: " + err);
			return false;
		}

		m_logger.info(getClass(), "Sent reboot message to node " + NodeID.toHexString(p_nodeID));
		return true;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == BootMessages.TYPE) {
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
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_boot = getComponent(AbstractBootComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		m_network.registerMessageType(BootMessages.TYPE, BootMessages.SUBTYPE_REBOOT_MESSAGE, RebootMessage.class);
		m_network.registerMessageType(BootMessages.TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE, ShutdownMessage.class);

		m_network.register(RebootMessage.class, this);
		m_network.register(ShutdownMessage.class, this);

		m_terminal.registerCommand(new TcmdNodeShutdown());
		m_terminal.registerCommand(new TcmdNodeReboot());

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
				} catch (final InterruptedException e) {
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
		if (p_message.isHardShutdown()) {
			// quick and painless
			System.exit(0);
		} else {
			DXRAMEngine parentEngine = getParentEngine();
			new Thread() {
				@Override
				public void run() {
					parentEngine.shutdown();
				}
			}.start();
		}
	}
}
