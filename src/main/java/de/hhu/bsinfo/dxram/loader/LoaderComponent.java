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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderComponent extends Component<LoaderComponentConfig> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoaderComponent.class);

    @Dependency
    private NetworkComponent m_net;
    @Dependency
    private BootComponent m_boot;
    @Dependency
    private EventComponent m_event;

    @Getter
    private DistributedLoader m_loader;
    private LoaderTable m_loaderTable;
    private String m_jarName;
    private NodeRole m_role;
    private final String m_loaderDir = "loadedJars";
    private Random m_random;

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

    public boolean addJarToLoader(Path p_jarPath) {
        int randomInt = getRandomNumberInRange(0, m_boot.getOnlineSuperpeerIds().size() - 1);
        short id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);

        try {
            byte[] jarBytes = Files.readAllBytes(p_jarPath);

            RegisterJarMessage registerJarMessage = new RegisterJarMessage(id, p_jarPath.toString(), jarBytes);
            m_net.sendMessage(registerJarMessage);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (NetworkException e) {
            LOGGER.error(e);
            return false;
        }
    }

    /**
     * Get jar from superpeer, this is called in the classloader
     *
     * @param p_name package name of the requested class
     * @return returns the path to the jar file
     * @throws ClassNotFoundException
     */
    public Path getJar(String p_name) throws ClassNotFoundException {
        int randomInt = getRandomNumberInRange(0, m_boot.getOnlineSuperpeerIds().size() - 1);
        short id = (short) m_boot.getOnlineSuperpeerIds().get(randomInt);

        ClassRequestMessage requestMessage = new ClassRequestMessage(id, p_name);
        try {
            m_net.sendMessage(requestMessage);
            while (m_jarName.equals("")) {
                Thread.yield();
            }

        } catch (NetworkException e) {
            LOGGER.error(e);
        }
        if (m_jarName.equals("NOT_FOUND")) {
            throw new ClassNotFoundException();
        }
        String jarName = m_jarName;
        m_jarName = "";

        return Paths.get(m_loaderDir + File.separator + jarName);
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
     * @param p_jarName name of the jar file
     * @param p_jarBytes byte array of the jar file
     */
    private void registerJarBytes(String p_jarName, byte[] p_jarBytes) {
        if (m_role.equals(NodeRole.SUPERPEER)) {
            m_loaderTable.registerJarBytes(p_jarName, p_jarBytes);
        } else {
            LOGGER.error("Only superpeers can register jars.");
        }

    }

    /**
     *
     * @return number of local registered jars
     */
    public int numberLoadedEntries(){
        if (m_role.equals(NodeRole.SUPERPEER)){
            return m_loaderTable.jarMapSize();
        }else{
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

        if (m_role.equals(NodeRole.PEER)) {
            m_jarName = "";
            m_loader = new DistributedLoader(Paths.get("dxapp"), this);

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_RESPONSE, h -> {
                ClassResponseMessage message = (ClassResponseMessage) h;
                String jarName = message.getM_jarName();

                if (!jarName.equals("NOT_FOUND")) {
                    try {
                        LOGGER.info("write file %s", m_loaderDir + File.separator + jarName);
                        Files.write(Paths.get(m_loaderDir + File.separator + jarName), message.getM_jarBytes());
                    } catch (IOException e) {
                        LOGGER.error(e);
                    }
                }

                m_jarName = jarName;
            });
        } else {
            m_loaderTable = new LoaderTable();

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, h -> {
                ClassRequestMessage requestMessage = (ClassRequestMessage) h;

                ClassResponseMessage responseMessage;
                try {
                    String jarName = m_loaderTable.getJarName(requestMessage.getM_packageName());

                    LOGGER.info(String.format("Found %s in %s, sending ClassResponseMessage",
                            requestMessage.getM_packageName(), jarName));
                    responseMessage = new ClassResponseMessage(requestMessage.getSource(), jarName,
                            m_loaderTable.getJarByte(jarName));

                } catch (NotInClusterException e) {
                    LOGGER.error(String.format("Class not found in cluster"));
                    responseMessage = new ClassResponseMessage(requestMessage.getSource(), "NOT_FOUND", new byte[1]);
                }

                try {
                    m_net.sendMessage(responseMessage);
                } catch (NetworkException e) {
                    LOGGER.error(e);
                }
            });

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER, h -> {
                RegisterJarMessage registerJarMessage = (RegisterJarMessage) h;

                if (!m_loaderTable.containsJar(registerJarMessage.getM_jarName())) {
                    registerJarBytes(registerJarMessage.getM_jarName(), registerJarMessage.getM_jarBytes());

                    List<Short> superPeers = m_boot.getOnlineSuperpeerIds();
                    //todo geOnlineSuperpeerIds should not contain own id?
                    superPeers.remove((Object) m_boot.getNodeId());
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
                }else{
                    LOGGER.info("The cluster already registered this jar.");
                }
            });

            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE, h -> {
                DistributeJarMessage distributeJarMessage = (DistributeJarMessage) h;
                registerJarBytes(distributeJarMessage.getM_jarName(), distributeJarMessage.getM_jarBytes());
            });

        }
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }
}
