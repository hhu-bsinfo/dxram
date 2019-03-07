package de.hhu.bsinfo.dxram.app;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.app.messages.ApplicationMessages;
import de.hhu.bsinfo.dxram.app.messages.ApplicationStartRequest;
import de.hhu.bsinfo.dxram.app.messages.ApplicationStartResponse;
import de.hhu.bsinfo.dxram.app.messages.ApplicationSubmitMessage;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Service to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 22.08.2018
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ApplicationService extends AbstractDXRAMService<ApplicationServiceConfig> implements MessageReceiver {
    // component dependencies
    private ApplicationComponent m_appComponent;
    private AbstractBootComponent m_bootComponent;
    private NetworkComponent m_networkComponent;

    private static final String ARG_SEPERATOR = "@";

    /**
     * Start an application
     *
     * @param p_class
     *         Fully qualified class name of application to start
     * @return True if starting application was successful, false on error
     */
    public boolean startApplication(final String p_class) {
        return m_appComponent.startApplication(p_class, new String[] {});
    }

    /**
     * Start an application
     *
     * @param p_class
     *         Fully qualified class name of application to start
     * @param p_args
     *         Arguments for application
     * @return True if starting application was successful, false on error
     */
    public boolean startApplication(final String p_class, final String[] p_args) {
        return m_appComponent.startApplication(p_class, p_args);
    }

    /**
     * Start an application on a remote peer
     *
     * @param p_nodeId
     *         Remote peer to run application on
     * @param p_class
     *         Fully qualified class name of application to start
     * @param p_args
     *         Arguments for application
     * @return True if starting application was successful, false on error
     */
    public boolean startApplication(final short p_nodeId, final String p_class, final String[] p_args) {
        if (m_bootComponent.getNodeId() == p_nodeId) {
            return m_appComponent.startApplication(p_class, p_args);
        } else {
            ApplicationStartRequest request = new ApplicationStartRequest(p_nodeId, p_class, p_args);

            try {
                m_networkComponent.sendSync(request);
            } catch (NetworkException e) {
                LOGGER.error("Sending start application request failed: %s", e.getMessage());
                return false;
            }

            return ((ApplicationStartResponse) request.getResponse()).isSuccess();
        }
    }

    /**
     * Shutdown a running application. This triggers the shutdown signal to allow the application
     * to initiate a soft shutdown
     *
     * @param p_class
     *         Fully qualified class name of application to shut down
     */
    public void shutdownApplication(final String p_class) {
        m_appComponent.shutdownApplication(p_class);
    }

    /**
     * Get a list of loaded application classes (available for starting)
     *
     * @return List of application classes loaded
     */
    public List<String> getLoadedApplicationClasses() {
        return m_appComponent.getLoadedApplicationClasses();
    }

    /**
     * Get a list of currently running applications
     *
     * @return List of currently running applications
     */
    public List<String> getApplicationsRunning() {
        return m_appComponent.getApplicationsRunning();
    }

    public Collection<ApplicationProcess> getRunningProcesses() {
        return m_appComponent.getRunningProcesses();
    }

    /**
     * Register an application class at the loader (external). Allows registering in-code
     * application classes without having to load jars (e.g. for testing).
     *
     * @param p_class
     *         Application class to register
     */
    public void registerApplicationClass(final Class<? extends AbstractApplication> p_class) {
        m_appComponent.registerApplicationClass(p_class);
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE) {
                switch (p_message.getSubtype()) {
                    case ApplicationMessages.SUBTYPE_START_APPLICATION_REQUEST:
                        incomingApplicationStartMessage((ApplicationStartRequest) p_message);
                        break;
                    case ApplicationMessages.SUBTYPE_SUBMIT_APPLICATION:
                        handleApplicationSubmit((ApplicationSubmitMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        LOGGER.trace("Exiting incomingMessage");
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_appComponent = p_componentAccessor.getComponent(ApplicationComponent.class);
        m_bootComponent = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_networkComponent = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_networkComponent.registerMessageType(DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_START_APPLICATION_REQUEST, ApplicationStartRequest.class);
        m_networkComponent.registerMessageType(DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_START_APPLICATION_RESPONSE, ApplicationStartResponse.class);
        m_networkComponent.registerMessageType(DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_SUBMIT_APPLICATION, ApplicationSubmitMessage.class);

        m_networkComponent.register(DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_START_APPLICATION_REQUEST, this);
        m_networkComponent.register(DXRAMMessageTypes.APPLICATION_MESSAGE_TYPE,
                ApplicationMessages.SUBTYPE_SUBMIT_APPLICATION, this);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    protected void engineInitFinished() {
        List<ApplicationServiceConfig.ApplicationEntry> list = getConfig().getAutoStart();
        list.sort(Comparator.comparingInt(ApplicationServiceConfig.ApplicationEntry::getStartOrderId));

        if (!list.isEmpty()) {
            LOGGER.info("Auto starting %d applications", list.size());
        }

        getConfig().getAutoStart().forEach(entry -> {
            String[] args;

            // to handle arguments passed using JVM args which use an @ to separate tokens because space doesn't work
            // not a very safe and elegant way to handle that...
            if (entry.getArgs().contains(ARG_SEPERATOR)) {
                args = entry.getArgs().split(ARG_SEPERATOR);
            } else {
                args = entry.getArgs().split(" ");
            }

            startApplication(entry.getClassName(), args);
        });
    }

    private void handleApplicationSubmit(final ApplicationSubmitMessage p_submitMessage) {
        LOGGER.debug("Received new application %s", p_submitMessage.getArchiveName());

        String archiveName = p_submitMessage.getArchiveName();
        String applicationName = m_appComponent.addApplication(archiveName);

        if (applicationName.isEmpty()) {
            LOGGER.warn("No subclass of AbstractApplication found within %s", archiveName);
            return;
        }

        startApplication(applicationName, p_submitMessage.getArgs());
    }

    private void incomingApplicationStartMessage(final ApplicationStartRequest p_request) {
        LOGGER.info("Remote 0x%X is starting application %s", p_request.getSource(), p_request.getName());

        boolean success = m_appComponent.startApplication(p_request.getName(), p_request.getArgs());

        ApplicationStartResponse resp = new ApplicationStartResponse(p_request, success);

        try {
            m_networkComponent.sendMessage(resp);
        } catch (NetworkException e) {
            LOGGER.error("Sending response to remote application start to 0x%X", p_request.getSource());
        }
    }
}
