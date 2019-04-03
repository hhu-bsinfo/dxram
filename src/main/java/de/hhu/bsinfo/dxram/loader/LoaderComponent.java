package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.event.Event;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.loader.messages.ClassRequestMessage;
import de.hhu.bsinfo.dxram.loader.messages.ClassResponseMessage;
import de.hhu.bsinfo.dxram.loader.messages.LoaderMessages;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderComponent extends Component<LoaderComponentConfig> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoaderComponent.class);

    @Dependency
    private NetworkComponent m_net;
    @Dependency
    private BootComponent m_boot;
    @Dependency
    private EventComponent m_event;

    private Event m_receiveEvent;

    @Getter
    private DistributedLoader m_loader;

    private ClassTable m_classtable;

    private String m_jarName;

    private NodeRole m_role;

    private final String m_loaderDir = "loadedJars";

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

    public Path getJar(String p_name) throws ClassNotFoundException {
        short id = (short) m_boot.getOnlineSuperpeerIds().get(0);
        ClassRequestMessage requestMessage = new ClassRequestMessage(id, p_name);
        try {
            m_net.sendMessage(requestMessage);
            while(m_jarName.equals("")){
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

    /**
     * For component testing, register jar local
     * @param p_jarPath
     */
    public void registerJar(Path p_jarPath){
        if(m_role.equals(NodeRole.SUPERPEER)) {
            m_classtable.registerJar(p_jarPath.toString());
        }else{
            LOGGER.error("Only superpeers can register jars.");
        }

    }

    @Override
    protected boolean initComponent(DXRAMConfig p_config, DXRAMJNIManager p_jniManager) {
        m_role = p_config.getEngineConfig().getRole();

        if (m_role.equals(NodeRole.PEER)) {
            m_jarName = "";
            m_loader = new DistributedLoader(Paths.get("dxapp"), this);
            m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, ClassRequestMessage.class);
            m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_RESPONSE, ClassResponseMessage.class);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_RESPONSE, h -> {
                ClassResponseMessage message = (ClassResponseMessage) h;
                String jarName = message.getM_jarName();

                if (!jarName.equals("NOT_FOUND")){
                    try {
                        LOGGER.info("write file %s", m_loaderDir + File.separator + jarName);
                        Files.write(Paths.get(m_loaderDir + File.separator + jarName), message.getM_jarBytes());
                    } catch (IOException e) {
                        LOGGER.error(e);
                    }
                }

                m_jarName = jarName;
            });
        }else{
            m_classtable = new ClassTable();

            m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, ClassRequestMessage.class);
            m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_RESPONSE, ClassResponseMessage.class);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST, h -> {
                ClassRequestMessage requestMessage = (ClassRequestMessage) h;
                String jarName = m_classtable.getJarName(requestMessage.getM_packageName());
                LOGGER.info(String.format("Found %s in %s", requestMessage.getM_packageName(), jarName));

                ClassResponseMessage responseMessage;
                if (jarName != null) {
                    File file = new File(jarName);
                    byte[] fileBytes = new byte[(int)file.length()];
                    try (FileInputStream fi = new FileInputStream(file)) {
                        fi.read(fileBytes);
                    }catch(FileNotFoundException e) {
                        e.printStackTrace();
                    }catch(IOException e) {
                        e.printStackTrace();
                    }

                    responseMessage = new ClassResponseMessage(requestMessage.getSource(), jarName, fileBytes);
                }else{
                    responseMessage = new ClassResponseMessage(requestMessage.getSource(), "NOT_FOUND", new byte[1]);
                }

                try {
                    LOGGER.info(String.format("Sending ClassResponseMessage with jar: %s", jarName));
                    m_net.sendMessage(responseMessage);
                    LOGGER.info(String.format("Sending %s completed", jarName));
                } catch (NetworkException e) {
                    LOGGER.error(e);
                }
            });

        }
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }
}
