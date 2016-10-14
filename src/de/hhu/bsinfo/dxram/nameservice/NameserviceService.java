
package de.hhu.bsinfo.dxram.nameservice;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.nameservice.messages.ForwardRegisterMessage;
import de.hhu.bsinfo.dxram.nameservice.messages.NameserviceMessages;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NameserviceService extends AbstractDXRAMService implements MessageReceiver {

	private NameserviceComponent m_nameservice;
	private AbstractBootComponent m_boot;
	private NetworkComponent m_network;
	private LoggerComponent m_logger;

	/**
	 * Constructor
	 */
	public NameserviceService() {
		super("name");
	}

	/**
	 * Register a chunk id for a specific name.
	 *
	 * @param p_chunkId Chunk id to register.
	 * @param p_name    Name to associate with the ID of the DataStructure.
	 */
	public void register(final long p_chunkId, final String p_name) {

		// any other nodes than peers cannot store this locally
		// (lacking the chunk service/memory block)
		// peers can also register chunkIDs which don't
		// have a valid NID (because they have the possibility to store
		// them in the index chunk). Other nodes have to find a peer
		// that can store the the nameservice entry
		// So the easiest solution was to simply require to have a valid NID
		// (which is the common case)
		if (m_boot.getNodeRole() != NodeRole.PEER) {
			// let each node manage its own index (the chunk part)
			short nodeId = ChunkID.getCreatorID(p_chunkId);
			if (nodeId == NodeID.INVALID_ID) {
				m_logger.error(getClass(),
						"Invalid creator id specified for registering " + ChunkID.toHexString(p_chunkId) + " for name "
								+ p_name);
				return;
			}

			if (m_boot.getNodeID() == nodeId) {
				m_nameservice.register(p_chunkId, p_name);
			} else {
				ForwardRegisterMessage message = new ForwardRegisterMessage(nodeId, p_chunkId, p_name);
				NetworkErrorCodes err = m_network.sendMessage(message);
				if (err != NetworkErrorCodes.SUCCESS) {
					m_logger.error(getClass(),
							"Sending register message to " + NodeID.toHexString(nodeId) + " failed: " + err);
				}
			}
		} else {
			m_nameservice.register(p_chunkId, p_name);
		}
	}

	/**
	 * Register a DataStructure for a specific name.
	 *
	 * @param p_dataStructure DataStructure to register.
	 * @param p_name          Name to associate with the ID of the DataStructure.
	 */
	public void register(final DataStructure p_dataStructure, final String p_name) {
		register(p_dataStructure.getID(), p_name);
	}

	/**
	 * Get the chunk ID of the specific name from the service.
	 *
	 * @param p_name      Registered name to get the chunk ID for.
	 * @param p_timeoutMs Timeout for trying to get the entry (if it does not exist, yet).
	 *                    set this to -1 for infinite loop if you know for sure, that the entry has to exist
	 * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
	 */
	public long getChunkID(final String p_name, final int p_timeoutMs) {
		return m_nameservice.getChunkID(p_name, p_timeoutMs);
	}

	/**
	 * Remove the name of a registered DataStructure from lookup.
	 *
	 * @return the number of entries in name service
	 */
	public int getEntryCount() {
		return m_nameservice.getEntryCount();
	}

	/**
	 * Get all available name mappings
	 *
	 * @return List of available name mappings
	 */
	public ArrayList<Pair<String, Long>> getAllEntries() {
		return m_nameservice.getAllEntries();
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == NameserviceMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case NameserviceMessages.SUBTYPE_REGISTER_MESSAGE:
						incomingRegisterMessage((ForwardRegisterMessage) p_message);
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
		m_nameservice = getComponent(NameserviceComponent.class);
		m_boot = getComponent(AbstractBootComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_logger = getComponent(LoggerComponent.class);

		m_network.registerMessageType(NameserviceMessages.TYPE, NameserviceMessages.SUBTYPE_REGISTER_MESSAGE,
				ForwardRegisterMessage.class);

		m_network.register(ForwardRegisterMessage.class, this);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_nameservice = null;
		m_boot = null;
		m_network = null;
		m_logger = null;

		return true;
	}

	/**
	 * Process an incoming RegisterMessage
	 *
	 * @param p_message Message to process
	 */
	private void incomingRegisterMessage(final ForwardRegisterMessage p_message) {
		m_nameservice.register(p_message.getChunkId(), p_message.getName());
	}
}
