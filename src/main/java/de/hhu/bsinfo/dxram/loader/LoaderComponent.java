/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.loader;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.loader.messages.ClassRequestMessage;
import de.hhu.bsinfo.dxram.loader.messages.ClassResponseMessage;
import de.hhu.bsinfo.dxram.loader.messages.DistributeJarMessage;
import de.hhu.bsinfo.dxram.loader.messages.FlushTableMessage;
import de.hhu.bsinfo.dxram.loader.messages.LoaderMessages;
import de.hhu.bsinfo.dxram.loader.messages.RegisterJarMessage;
import de.hhu.bsinfo.dxram.loader.messages.SyncInvitationMessage;
import de.hhu.bsinfo.dxram.loader.messages.SyncRequestMessage;
import de.hhu.bsinfo.dxram.loader.messages.SyncResponseMessage;
import de.hhu.bsinfo.dxram.loader.messages.UpdateMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponentConfig;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderComponent extends Component<LoaderComponentConfig> implements MessageReceiver {
    @Dependency
    private NetworkComponent m_net;
    @Dependency
    private BootComponent m_boot;
    @Dependency
    private LookupComponent m_lookup;

    @Getter
    private DistributedLoader m_loader;
    private LoaderTable m_loaderTable;
    private NodeRole m_role;
    private Random m_random;
    private static final String CLASS_NOT_FOUND = "NOT_FOUND";
    private Path m_pluginPath;

    /**
     * Clean folder with requested jars from loader
     */
    private void cleanLoaderDir() {
        try {
            Files.walk(Paths.get(getConfig().getLoaderDir()))
                    .sorted(Comparator.reverseOrder())
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .forEach(File::delete);

            LOGGER.info("Loader dir cleanup finished.");
        } catch (IOException e) {
            LOGGER.error("Loader dir cleanup failed ", e);
        }
    }

    /**
     * Request loaderTable sync with random superpeer.
     */
    public void sync() {
        List<Short> superPeers = m_boot.getOnlineSuperpeerIds();
        superPeers.remove((Short) m_boot.getNodeId());

        if (superPeers.isEmpty()) {
            LOGGER.info("Nothing to sync, only one superpeer.");
        }else {
            int randomInt = getRandomInt(superPeers.size());
            short id = superPeers.get(randomInt);
            LOGGER.info(String.format("request loaderTable sync with %s", NodeID.toHexString(id)));
            SyncRequestMessage syncRequestMessage = new SyncRequestMessage(id, m_loaderTable.getLoadedJars());

            try {
                m_net.sendMessage(syncRequestMessage);
            } catch (NetworkException e) {
                LOGGER.error(e);
            }
        }
    }

    /**
     * Only for testing, flushes all maps on superpeer.
     */
    public void flushTable() {
        LOGGER.info(String.format("loaderTable on %s flushed.", NodeID.toHexString(m_boot.getNodeId())));
        m_loaderTable.flushMaps();
    }

    /**
     * Add jar file from specific path to cluster, after successful operation, the package is on all superpeers
     *
     * @param p_jarPath
     *         path to jar file
     * @return true if successful
     */
    public boolean addJarToLoader(Path p_jarPath) {
        short id;
        if (getConfig().isRandomRequest()) {
            int randomInt = getRandomInt(m_boot.getOnlineSuperpeerIds().size());
            id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);
        } else {
            id = m_lookup.getResponsibleSuperpeer(m_boot.getNodeId());
        }
        LOGGER.info(String.format("Sending %s to %s", p_jarPath, NodeID.toHexString(id)));

        try {
            byte[] jarBytes = Files.readAllBytes(p_jarPath);
            String name = p_jarPath.getFileName().toString().replace(".jar", "");
            int version = 0;

            if (name.contains("-")) {
                int sep = name.indexOf('-');
                version = Integer.parseInt(name.substring(sep + 1));
                name = name.substring(0, sep);
            }

            LoaderJar loaderJar = new LoaderJar(jarBytes, version, name);

            RegisterJarMessage registerJarMessage = new RegisterJarMessage(id, loaderJar);
            m_net.sendMessage(registerJarMessage);
            return true;
        } catch (IOException e) {
            LOGGER.error(e);
            return false;
        } catch (NetworkException e) {
            LOGGER.error("Could not send message ", e);
            return false;
        }
    }

    /**
     * Get jar from superpeer, this is called in the classloader
     *
     * @param p_name
     *         package name of the requested class
     * @return returns the path to the jar file
     * @throws ClassNotFoundException
     *         class not found in cluster
     */
    public void getJar(String p_name) throws ClassNotFoundException {
        LOGGER.info(String.format("Ask LoaderComponent for %s", p_name));

        short id;
        if (getConfig().isRandomRequest()) {
            int randomInt = getRandomInt(m_boot.getOnlineSuperpeerIds().size());
            id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);
        } else {
            id = m_lookup.getResponsibleSuperpeer(m_boot.getNodeId());
        }

        ClassRequestMessage requestMessage = new ClassRequestMessage(id, p_name);

        int count = 0;
        while(true) {
            try {
                m_net.sendSync(requestMessage, true);

                ClassResponseMessage response = (ClassResponseMessage) requestMessage.getResponse();

                if (CLASS_NOT_FOUND.equals(response.getM_loaderJar().getM_name())) {
                    throw new ClassNotFoundException();
                }

                Path jarPath = Paths.get(getConfig().getLoaderDir() + File.separator +
                        response.getM_loaderJar().getM_name() + ".jar");
                response.getM_loaderJar().writeToPath(jarPath);

                m_loader.add(jarPath);
                LOGGER.info(String.format("Added %s to ClassLoader", p_name));

                break;
            } catch (ClassNotFoundException | NetworkException e) {
                if (++count == getConfig().getMaxTries()) {
                    throw new ClassNotFoundException();
                }else {
                    LOGGER.info(String.format("Retry getting %s in %s ms... (Retry number %s)",
                            p_name, getConfig().m_retryInterval, count));
                    try {
                        TimeUnit.MILLISECONDS.sleep(getConfig().m_retryInterval);
                    } catch (InterruptedException ex) {
                        LOGGER.error(ex);
                    }
                }
            }
        }
    }

    private int getRandomInt(int p_max) {
        if (p_max == 0) {
            return 0;
        }
        if (p_max <= 0) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return m_random.nextInt(p_max);
    }

    /**
     * Register a jar file from a RegisterJarMessage or a DistributeJarMessage (on superpeer)
     *
     * @param p_loaderJar
     *         jar object with version
     */
    private void registerJarBytes(LoaderJar p_loaderJar) {
        if (m_role == NodeRole.SUPERPEER) {
            m_loaderTable.registerJarBytes(p_loaderJar);
        } else {
            LOGGER.error("Only superpeers can register jars.");
        }
    }

    /**
     * @return number of local registered jars
     */
    public int getLocalLoadedCount() {
        if (m_role == NodeRole.SUPERPEER) {
            return m_loaderTable.jarMapSize();
        } else {
            LOGGER.warn("Only superpeers contain jar byte arrays");
            return 0;
        }
    }

    /**
     * Update an app, this creates a new Loader and loads all apps in pluginPath and loaderDir
     *
     * @param p_loaderJar
     *         jar to update
     */
    private void updateApp(LoaderJar p_loaderJar) {
        try {
            Files.write(Paths.get(getConfig().getLoaderDir() + File.separator + p_loaderJar.getM_name() + ".jar"),
                    p_loaderJar.getM_jarBytes());

            DistributedLoader newLoader = new DistributedLoader(this);
            newLoader.initPlugins(m_pluginPath);
            newLoader.initPlugins(Paths.get(getConfig().getLoaderDir()));

            m_loader = newLoader;
            LOGGER.info(String.format("Updated %s to version %s", p_loaderJar.getM_name(), p_loaderJar.getM_version()));
        } catch (IOException e) {
            LOGGER.error(String.format("Updating %s failed: %s", p_loaderJar.getM_name(), e));
        }
    }

    @Override
    protected boolean initComponent(DXRAMConfig p_config, DXRAMJNIManager p_jniManager) {
        PluginComponentConfig config = p_config.getComponentConfig(PluginComponent.class);
        m_pluginPath = Paths.get(config.getPluginsPath());

        m_random = new Random();
        m_role = p_config.getEngineConfig().getRole();
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST,
                ClassRequestMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_RESPONSE,
                ClassResponseMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER,
                RegisterJarMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE,
                DistributeJarMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST,
                SyncRequestMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE,
                SyncResponseMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_INVITATION,
                SyncInvitationMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_UPDATE,
                UpdateMessage.class);

        if (m_role == NodeRole.PEER) {
            if (!Files.exists(Paths.get(getConfig().getLoaderDir()))) {
                try {
                    Files.createDirectory(Paths.get(getConfig().getLoaderDir()));
                } catch (IOException e) {
                    LOGGER.error("Could not create loaderDir.", e);
                }
            }

            m_loader = new DistributedLoader(this);

            if (ClassLoader.getSystemClassLoader() instanceof DistributedSystemLoader) {
                LOGGER.info("DistributedLoader registered in SystemClassLoader");
                ((DistributedSystemLoader) ClassLoader.getSystemClassLoader()).setDistributedLoader(m_loader);
            } else {
                LOGGER.warn("DistributedSystemLoader is not the SystemClassLoader," +
                        " it will only work with Applications and the LoaderService. Please use the vm argument" +
                        " '-Djava.system.class.loader=de.hhu.bsinfo.dxram.loader.DistributedSystemLoader'");

            }

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_UPDATE, this);
        } else {
            m_loaderTable = new LoaderTable();

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_INVITATION, this);
        }
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_role == NodeRole.SUPERPEER) {
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_INVITATION, this);
        } else {
            cleanLoaderDir();
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_UPDATE, this);
        }

        return true;
    }

    /**
     * Lookup of jar for requested package and send response with jar or CLASS_NOT_FOUND
     * If forceSyncWhenNotFound is true, send SyncRequest to random superpeer, if class is not found
     *
     * @param p_message
     *         message with request
     */
    private void onIncomingClassRequest(Message p_message) {
        ClassRequestMessage requestMessage = (ClassRequestMessage) p_message;

        ClassResponseMessage responseMessage;
        try {
            String jarName = m_loaderTable.getJarName(requestMessage.getM_packageName());

            LOGGER.info(String.format("Found %s in %s, sending ClassResponseMessage",
                    requestMessage.getM_packageName(), jarName));
            responseMessage = new ClassResponseMessage(requestMessage, m_loaderTable.getLoaderJar(jarName));
            m_loaderTable.logClassRequest(requestMessage.getSource(), jarName);

        } catch (NotInClusterException e) {
            LOGGER.error("Class not found in cluster");
            responseMessage = new ClassResponseMessage(requestMessage, new LoaderJar(CLASS_NOT_FOUND));
            if (getConfig().m_forceSyncWhenNotFound) {
                sync();
            }
        }

        try {
            m_net.sendMessage(responseMessage);
        } catch (NetworkException e) {
            LOGGER.error(e);
        }
    }

    /**
     * Register jar byte array from peer and distribute to other superpeers
     *
     * @param p_message
     *         message with request
     */
    private void onIncomingClassRegister(Message p_message) {
        RegisterJarMessage registerJarMessage = (RegisterJarMessage) p_message;

        if (!m_loaderTable.containsJar(registerJarMessage.getM_loaderJar().getM_name())) {
            registerJarBytes(registerJarMessage.getM_loaderJar());
            distributeJar(registerJarMessage.getM_loaderJar());
        } else {
            // check if the jar is a newer version
            if (m_loaderTable.getLoaderJar(registerJarMessage.getM_loaderJar().getM_name()).getM_version() <
                    registerJarMessage.getM_loaderJar().getM_version()) {
                registerJarBytes(registerJarMessage.getM_loaderJar());
                distributeJar(registerJarMessage.getM_loaderJar());

                if (getConfig().isAutoUpdate()) {
                    pushNewVersion(registerJarMessage.getM_loaderJar());
                }
            } else {
                LOGGER.info("The cluster already registered this jar.");
            }
        }

        m_loaderTable.logClassRequest(registerJarMessage.getSource(), registerJarMessage.getM_loaderJar().getM_name());
    }

    private void pushNewVersion(LoaderJar p_newVersion) {
        List<Short> peers = m_loaderTable.peersWithJar(p_newVersion.getM_name());
        if (!peers.isEmpty()) {
            LOGGER.info(String.format("Sending updated %s jar to: %s",
                    p_newVersion.getM_name(), peers));
            for (short peer : peers) {
                UpdateMessage updateMessage = new UpdateMessage(peer, p_newVersion);

                try {
                    m_net.sendMessage(updateMessage);
                } catch (NetworkException e) {
                    LOGGER.error(e);
                }
            }
        }
    }

    private void distributeJar(LoaderJar p_loaderJar) {
        List<Short> superPeers = m_boot.getOnlineSuperpeerIds();
        superPeers.remove((Short) m_boot.getNodeId());
        if (!superPeers.isEmpty()) {
            LOGGER.info(String.format("Distribute %s to other superpeers: %s", p_loaderJar.getM_name(), superPeers));
            for (Short superPeer : superPeers) {
                DistributeJarMessage distributeJarMessage = new DistributeJarMessage(superPeer, p_loaderJar,
                        m_loaderTable.jarMapSize());
                try {
                    m_net.sendMessage(distributeJarMessage);
                } catch (NetworkException e) {
                    LOGGER.error(e);
                }
            }
        }
    }

    /**
     * Register jar byte array from distribute message
     *
     * @param p_message
     *         message with request
     */
    private void onIncomingClassDistribute(Message p_message) {
        DistributeJarMessage distributeJarMessage = (DistributeJarMessage) p_message;

        if (!m_loaderTable.containsJar(distributeJarMessage.getM_loaderJar().getM_name())) {
            registerJarBytes(distributeJarMessage.getM_loaderJar());
        } else {
            // check if the jar is a newer version
            if (m_loaderTable.getLoaderJar(distributeJarMessage.getM_loaderJar().getM_name()).getM_version() <
                    distributeJarMessage.getM_loaderJar().getM_version()) {
                registerJarBytes(distributeJarMessage.getM_loaderJar());

                if (getConfig().isAutoUpdate()) {
                    pushNewVersion(distributeJarMessage.getM_loaderJar());
                }
            } else {
                LOGGER.info("The cluster already registered this jar.");
            }
        }

        if (distributeJarMessage.getM_tableSize() > m_loaderTable.jarMapSize()) {
            LOGGER.info(String.format("loaderTable is not synced (size is %s and should be %s), request sync",
                    m_loaderTable.jarMapSize(), distributeJarMessage.getM_tableSize()));

            SyncRequestMessage syncRequestMessage = new SyncRequestMessage(distributeJarMessage.getSource(),
                    m_loaderTable.getLoadedJars());

            try {
                m_net.sendMessage(syncRequestMessage);
            } catch (NetworkException e) {
                LOGGER.error(e);
            }
        } else if (distributeJarMessage.getM_tableSize() < m_loaderTable.jarMapSize()) {
            LOGGER.info(String.format("other peers loaderTable is not synced (size is %s and should be %s)," +
                            " invite for sync",
                    distributeJarMessage.getM_tableSize(), m_loaderTable.jarMapSize()));

            SyncInvitationMessage syncInvitationMessage = new SyncInvitationMessage(distributeJarMessage.getSource());

            try {
                m_net.sendMessage(syncInvitationMessage);
            } catch (NetworkException e) {
                LOGGER.error(e);
            }
        } else {
            LOGGER.info("loaderTable is synced");
        }
    }

    /**
     * Checks which jars are not loaded on the other superpeer and sends these to the superpeer
     *
     * @param p_message
     *         message with request
     */
    private void onIncomingSyncRequest(Message p_message) {
        SyncRequestMessage requestMessage = (SyncRequestMessage) p_message;

        Set<String> loadedJars = m_loaderTable.getLoadedJars();
        loadedJars.removeAll(requestMessage.getLoadedJars());

        if (!loadedJars.isEmpty()) {
            LOGGER.info(String.format("Other peers needs %s", loadedJars));

            LoaderJar[] responseArray = new LoaderJar[loadedJars.size()];
            int i = 0;
            for (String jarName : loadedJars) {
                responseArray[i] = m_loaderTable.getLoaderJar(jarName);
                i++;
            }

            SyncResponseMessage response = new SyncResponseMessage(requestMessage.getSource(), responseArray);
            LOGGER.info(String.format("Sending SyncResponseMessage with %s jars", responseArray.length));
            try {
                m_net.sendMessage(response);
            } catch (NetworkException e) {
                LOGGER.error(e);
            }
        } else {
            LOGGER.info("The peer has all jars, abort sync");
        }

    }

    /**
     * Register all the missing jars
     *
     * @param p_message
     *         message with response
     */
    private void onIncomingSyncResponse(Message p_message) {
        SyncResponseMessage responseMessageMessage = (SyncResponseMessage) p_message;

        m_loaderTable.registerJarMap(responseMessageMessage.getJarByteArrays());
    }

    private void onIncomingSyncInvitation(Message p_message) {
        SyncRequestMessage syncRequestMessage = new SyncRequestMessage(p_message.getSource(),
                m_loaderTable.getLoadedJars());

        try {
            m_net.sendMessage(syncRequestMessage);
        } catch (NetworkException e) {
            LOGGER.error(e);
        }
    }

    /**
     * Update jar on peer with new version from message
     *
     * @param p_message message with response
     */
    private void onIncomingUpdateMessage(Message p_message) {
        UpdateMessage updateMessage = (UpdateMessage) p_message;

        updateApp(updateMessage.getM_loaderJar());
    }

    @Override
    public void onIncomingMessage(Message p_message) {
        switch (p_message.getSubtype()) {
            case LoaderMessages.SUBTYPE_CLASS_REQUEST:
                onIncomingClassRequest(p_message);
                break;
            case LoaderMessages.SUBTYPE_CLASS_REGISTER:
                onIncomingClassRegister(p_message);
                break;
            case LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE:
                onIncomingClassDistribute(p_message);
                break;
            case LoaderMessages.SUBTYPE_SYNC_REQUEST:
                onIncomingSyncRequest(p_message);
                break;
            case LoaderMessages.SUBTYPE_SYNC_RESPONSE:
                onIncomingSyncResponse(p_message);
                break;
            case LoaderMessages.SUBTYPE_SYNC_INVITATION:
                onIncomingSyncInvitation(p_message);
                break;
            case LoaderMessages.SUBTYPE_UPDATE:
                onIncomingUpdateMessage(p_message);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + p_message.getSubtype());
        }
    }
}
