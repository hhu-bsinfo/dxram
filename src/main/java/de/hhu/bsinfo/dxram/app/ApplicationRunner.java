package de.hhu.bsinfo.dxram.app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Runner to run applications and keep track of already running ones
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.18
 */
public class ApplicationRunner implements ApplicationCallbackHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationRunner.class);

    private final HashMap<String, Class<? extends AbstractApplication>> m_applicationClasses;
    private final DXRAMVersion m_dxramVersion;
    private final DXRAMServiceAccessor m_dxramServiceAccessor;

    private HashMap<String, ApplicationProcess> m_runningProcesses;

    private static final int MAX_PROCESSES = 1000;
    private ConcurrentSkipListSet<Integer> m_processIds = new ConcurrentSkipListSet<>(
            IntStream.range(0, MAX_PROCESSES).boxed().collect(Collectors.toList()));

    /**
     * Constructor
     *
     * @param p_applicationClasses
     *         Map of available application classes
     * @param p_dxramVersion
     *         Version of DXRAM running on
     * @param p_dxramServiceAccessor
     *         DXRAM service accessor
     */
    ApplicationRunner(final HashMap<String, Class<? extends AbstractApplication>> p_applicationClasses,
            final DXRAMVersion p_dxramVersion, final DXRAMServiceAccessor p_dxramServiceAccessor) {
        m_applicationClasses = p_applicationClasses;
        m_dxramVersion = p_dxramVersion;
        m_dxramServiceAccessor = p_dxramServiceAccessor;

        m_runningProcesses = new HashMap<>();
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
        if (p_class.isEmpty()) {
            return false;
        }

        if (m_runningProcesses.get(p_class) != null) {
            LOGGER.error("Cannot start application '%s', an instance is already running", p_class);
            return false;
        }

        Class<? extends AbstractApplication> appClass = m_applicationClasses.get(p_class);

        if (appClass == null) {
            LOGGER.warn("Application class %s was not found", p_class);
            return false;
        }

        AbstractApplication app;

        try {
            app = appClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            LOGGER.error("Creating instance of application class %s failed: %s", appClass.getName(),
                    e.getMessage());
            return false;
        }

        // verify if built against current version
        if (!app.getBuiltAgainstVersion().compareCompatibility(m_dxramVersion)) {
            LOGGER.error("Cannot start application '%s' with version %s, not compatible with current DXRAM " +
                    "version %s", app.getName(), app.getBuiltAgainstVersion(), m_dxramVersion);

            return false;
        }

        app.init(m_dxramServiceAccessor, this, p_args);

        LOGGER.info("Starting %s", app.getApplicationName());

        app.start();

        return true;
    }

    /**
     * Shutdown a running application. This triggers the shutdown signal to allow the application
     * to initiate a soft shutdown
     *
     * @param p_class
     *         Fully qualified class name of application to shut down
     */
    public boolean shutdownApplication(final String p_class) {
        ApplicationProcess process = m_runningProcesses.get(p_class);

        if (process == null) {
            LOGGER.warn("Shutting down application '%s' failed, no running instance found", p_class);
            return false;
        }

        LOGGER.debug("Signaling shutdown to application %s", p_class);

        process.kill();

        return true;
    }

    /**
     * Get a list of currently running applications
     *
     * @return List of currently running applications
     */
    public List<String> getApplicationsRunning() {
        return new ArrayList<>(m_runningProcesses.keySet());
    }

    public Collection<ApplicationProcess> getRunningProcesses() {
        return m_runningProcesses.values();
    }

    /**
     * Shut down the runner which signals a shutdown to still running applications and waiting for
     * their termination
     */
    public void shutdown() {
        LOGGER.info("Shutting down runner, signaling shut down to all running applications first");

        for (Map.Entry<String, ApplicationProcess> entry : m_runningProcesses.entrySet()) {
            entry.getValue().kill();
        }

        LOGGER.info("Waiting for applications to finish");

        for (Map.Entry<String, ApplicationProcess> entry : m_runningProcesses.entrySet()) {
            LOGGER.debug("Waiting for %s...", entry.getKey());

            try {
                entry.getValue().join();
            } catch (InterruptedException ignored) {

            }
        }
    }

    @Override
    public void started(final AbstractApplication p_application) {
        LOGGER.debug("Application started: %s", p_application);

        Integer processId = m_processIds.pollFirst();
        if (processId == null) {
            throw new IllegalStateException("Couldn't retrieve next process id");
        }

        ApplicationProcess process = new ApplicationProcess(processId, p_application);
        m_runningProcesses.put(p_application.getClass().getName(), process);
    }

    @Override
    public void finished(final AbstractApplication p_application) {
        LOGGER.debug("Application finished: %s", p_application);

        ApplicationProcess process = m_runningProcesses.remove(p_application.getClass().getName());
        m_processIds.add(process.getId());
    }
}
