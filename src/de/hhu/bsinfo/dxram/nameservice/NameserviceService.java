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

package de.hhu.bsinfo.dxram.nameservice;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.nameservice.messages.ForwardRegisterMessage;
import de.hhu.bsinfo.dxram.nameservice.messages.NameserviceMessages;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NameserviceService extends AbstractDXRAMService implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NameserviceService.class.getSimpleName());

    // component dependencies
    private NameserviceComponent m_nameservice;
    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public NameserviceService() {
        super("name");
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
    public ArrayList<NameserviceEntryStr> getAllEntries() {
        return m_nameservice.getAllEntries();
    }

    /**
     * Register a chunk id for a specific name.
     *
     * @param p_chunkId
     *         Chunk id to register.
     * @param p_name
     *         Name to associate with the ID of the DataStructure.
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
                // #if LOGGER >= ERROR
                LOGGER.error("Invalid creator id specified for registering 0x%X for name %s", p_chunkId, p_name);
                // #endif /* LOGGER >= ERROR */
                return;
            }

            if (m_boot.getNodeID() == nodeId) {
                m_nameservice.register(p_chunkId, p_name);
            } else {
                ForwardRegisterMessage message = new ForwardRegisterMessage(nodeId, p_chunkId, p_name);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending register message to 0x%X failed: %s", nodeId, e);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } else {
            m_nameservice.register(p_chunkId, p_name);
        }
    }

    /**
     * Register a DataStructure for a specific name.
     *
     * @param p_dataStructure
     *         DataStructure to register.
     * @param p_name
     *         Name to associate with the ID of the DataStructure.
     */
    public void register(final DataStructure p_dataStructure, final String p_name) {
        register(p_dataStructure.getID(), p_name);
    }

    /**
     * Get the chunk ID of the specific name from the service.
     *
     * @param p_name
     *         Registered name to get the chunk ID for.
     * @param p_timeoutMs
     *         Timeout for trying to get the entry (if it does not exist, yet).
     *         set this to -1 for infinite loop if you know for sure, that the entry has to exist
     * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
     */
    public long getChunkID(final String p_name, final int p_timeoutMs) {
        return m_nameservice.getChunkID(p_name, p_timeoutMs);
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.NAMESERVICE_MESSAGES_TYPE) {
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
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_network.registerMessageType(DXRAMMessageTypes.NAMESERVICE_MESSAGES_TYPE, NameserviceMessages.SUBTYPE_REGISTER_MESSAGE, ForwardRegisterMessage.class);

        m_network.register(ForwardRegisterMessage.class, this);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Process an incoming RegisterMessage
     *
     * @param p_message
     *         Message to process
     */
    private void incomingRegisterMessage(final ForwardRegisterMessage p_message) {
        // Outsource registering to another thread to avoid blocking a message handler
        Runnable task = () -> m_nameservice.register(p_message.getChunkId(), p_message.getName());
        new Thread(task).start();
    }
}
