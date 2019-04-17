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

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.loader.messages.ClassRequestMessage;
import de.hhu.bsinfo.dxram.loader.messages.ClassResponseMessage;
import de.hhu.bsinfo.dxram.loader.messages.DistributeJarMessage;
import de.hhu.bsinfo.dxram.loader.messages.LoaderMessages;
import de.hhu.bsinfo.dxram.loader.messages.RegisterJarMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderComponent extends Component<LoaderComponentConfig> implements MessageReceiver {
    @Dependency
    private NetworkComponent m_net;
    @Dependency
    private BootComponent m_boot;
    @Dependency
    private EventComponent m_event;
    @Dependency
    private LookupComponent m_lookup;

    @Getter
    private DistributedLoader m_loader;
    private LoaderTable m_loaderTable;
    private NodeRole m_role;
    private final String m_loaderDir = "loadedJars";
    private Random m_random;
    final private String CLASS_NOT_FOUND = "NOT_FOUND";

    /**
     * Clean folder with requested jars from loader
     */
    public void cleanLoaderDir() {
        try {
            Files.walk(Paths.get(m_loaderDir))
                    .sorted(Comparator.reverseOrder())
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            LOGGER.error(e);
        }

        LOGGER.info("Loader dir cleanup finished.");
    }

    /**
     * Add jar file from specific path to cluster, after successful operation, the package is on all superpeers
     *
     * @param p_jarPath
     *         path to jar file
     * @return
     */
    public boolean addJarToLoader(Path p_jarPath) {
        int randomInt = getRandomNumberInRange(0, m_boot.getOnlineSuperpeerIds().size() - 1);
        short id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);

        try {
            byte[] jarBytes = Files.readAllBytes(p_jarPath);

            RegisterJarMessage registerJarMessage = new RegisterJarMessage(id, p_jarPath.getFileName().toString(), jarBytes);
            m_net.sendMessage(registerJarMessage);
            return true;
        } catch (IOException e) {
            LOGGER.error(e);
            return false;
        } catch (NetworkException e) {
            LOGGER.error(e);
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
    public Path getJar(String p_name) throws ClassNotFoundException {
        LOGGER.info(String.format("Ask LoaderComponent for %s", p_name));

        int randomInt = getRandomNumberInRange(0, m_boot.getOnlineSuperpeerIds().size() - 1);
        short id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);

        ClassRequestMessage requestMessage = new ClassRequestMessage(id, p_name);
        try {
            m_net.sendSync(requestMessage, true);

        } catch (NetworkException e) {
            LOGGER.error(e);
        }
        ClassResponseMessage response = (ClassResponseMessage) requestMessage.getResponse();

        if (CLASS_NOT_FOUND.equals(response.getM_jarName())) {
            throw new ClassNotFoundException();
        } else {
            try {
                LOGGER.info(String.format("write file %s", m_loaderDir + File.separator + response.getM_jarName()));
                Files.write(Paths.get(m_loaderDir + File.separator + response.getM_jarName()),
                        response.getM_jarBytes());
            } catch (IOException e) {
                LOGGER.error(e);
            }
        }

        LOGGER.info(String.format("Added %s to ClassLoader", p_name));
        return Paths.get(m_loaderDir + File.separator + response.getM_jarName());
    }

    private int getRandomNumberInRange(int min, int max) {
        if (max == 0) {
            return 0;
        } else if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        return m_random.nextInt(max - min + 1) + min;
    }

    /**
     * Register a jar file from a RegisterJarMessage or a DistributeJarMessage (on superpeer)
     *
     * @param p_jarName
     *         name of the jar file
     * @param p_jarBytes
     *         byte array of the jar file
     */
    private void registerJarBytes(String p_jarName, byte[] p_jarBytes) {
        if (m_role == NodeRole.SUPERPEER) {
            m_loaderTable.registerJarBytes(p_jarName, p_jarBytes);
        } else {
            LOGGER.error("Only superpeers can register jars.");
        }

    }

    /**
     * @return number of local registered jars
     */
    public int numberLoadedEntries() {
        if (m_role == NodeRole.SUPERPEER) {
            return m_loaderTable.jarMapSize();
        } else {
            LOGGER.warn("Only superpeers contain jar byte arrays");
            return 0;
        }
    }

    @Override
    protected boolean initComponent(DXRAMConfig p_config, DXRAMJNIManager p_jniManager) {
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

        if (m_role == NodeRole.PEER) {
            if (!Files.exists(Paths.get(m_loaderDir))){
                try {
                    Files.createDirectory(Paths.get(m_loaderDir));
                } catch (IOException e) {
                    LOGGER.error("Could not create loaderDir.", e);
                }
            }
            if (ClassLoader.getSystemClassLoader() instanceof DistributedLoader) {
                LOGGER.info("DistributedLoader is SystemClassLoader.");
                m_loader = (DistributedLoader) ClassLoader.getSystemClassLoader();
            }else {
                LOGGER.warn("DistributedClassloader is not SystemClassLoader, it will only work with Applications" +
                        " and the LoaderService. Please use the vm argument" +
                        " '-Djava.system.class.loader=de.hhu.bsinfo.dxram.loader.DistributedLoader'");
                m_loader = new DistributedLoader();
            }
            m_loader.registerLoaderComponent(this);
        } else {
            m_loaderTable = new LoaderTable();

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE, this);
        }
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_role == NodeRole.SUPERPEER) {
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE, this);
        }else {
            cleanLoaderDir();
        }

        return true;
    }

    /**
     * Lookup of jar for requested package and send response with jar or CLASS_NOT_FOUND
     *
     * @param p_message
     */
    private void onIncomingClassRequest(Message p_message) {
        ClassRequestMessage requestMessage = (ClassRequestMessage) p_message;

        ClassResponseMessage responseMessage;
        try {
            String jarName = m_loaderTable.getJarName(requestMessage.getM_packageName());

            LOGGER.info(String.format("Found %s in %s, sending ClassResponseMessage",
                    requestMessage.getM_packageName(), jarName));
            responseMessage = new ClassResponseMessage(requestMessage, jarName,
                    m_loaderTable.getJarByte(jarName));

        } catch (NotInClusterException e) {
            LOGGER.error("Class not found in cluster");
            responseMessage = new ClassResponseMessage(requestMessage,
                    CLASS_NOT_FOUND, new byte[0]);
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
     */
    private void onIncomingClassRegister(Message p_message) {
        RegisterJarMessage registerJarMessage = (RegisterJarMessage) p_message;

        if (!m_loaderTable.containsJar(registerJarMessage.getM_jarName())) {
            registerJarBytes(registerJarMessage.getM_jarName(), registerJarMessage.getM_jarBytes());

            List<Short> superPeers = m_boot.getOnlineSuperpeerIds();
            //todo geOnlineSuperpeerIds should not contain own id?
            superPeers.remove((Short) m_boot.getNodeId());
            LOGGER.info(String.format("Distribute %s to other superpeers: %s",
                    registerJarMessage.getM_jarName(), superPeers));
            for (Short superPeer : superPeers) {
                DistributeJarMessage distributeJarMessage = new DistributeJarMessage(superPeer,
                        registerJarMessage.getM_jarName(), registerJarMessage.getM_jarBytes());
                try {
                    m_net.sendMessage(distributeJarMessage);
                } catch (NetworkException e) {
                    LOGGER.error(e);
                }
            }
        } else {
            LOGGER.info("The cluster already registered this jar.");
        }
    }

    /**
     * Register jar byte array from distribute message
     *
     * @param p_message
     */
    private void onIncomingClassDistribute(Message p_message) {
        DistributeJarMessage distributeJarMessage = (DistributeJarMessage) p_message;
        registerJarBytes(distributeJarMessage.getM_jarName(), distributeJarMessage.getM_jarBytes());
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
        }
    }
}
