/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.engine;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.generated.BuildConfig;

/**
 * Engine class running DXRAM with components and services.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DXRAMEngine implements DXRAMServiceAccessor, DXRAMComponentAccessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMEngine.class);

    private final DXRAMVersion m_version;
    private final DXRAMModuleManager m_componentManager;
    private final DXRAMModuleManager m_serviceManager;

    private boolean m_isInitialized;
    private DXRAMConfig m_config;
    private DXRAMJNIManager m_jniManager;

    private final BlockingQueue<EngineEvent> m_events = new LinkedBlockingQueue<>();

    private final Lock m_engineLock = new ReentrantLock();

    enum EngineEvent {
        REBOOT, SHUTDOWN
    }

    /**
     * Constructor
     *
     * @param p_version
     *         Object to label the current version
     */
    public DXRAMEngine(final DXRAMVersion p_version) {
        m_version = Objects.requireNonNull(p_version);
        m_componentManager = new DXRAMModuleManager();
        m_serviceManager = new DXRAMModuleManager();
    }

    /**
     * Get the version of the current DXRAM (engine) instance
     *
     * @return Version of current DXRAM instance
     */
    public DXRAMVersion getVersion() {
        return m_version;
    }

    /**
     * Register a DXRAM component
     *
     * @param p_class
     *         Class of the component to register
     * @param p_configClass
     *         Configuration class to associate with the component
     */
    public void registerComponent(final Class<? extends AbstractDXRAMComponent> p_class,
            final Class<? extends DXRAMModuleConfig> p_configClass) {
        if (m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        m_componentManager.register(p_class, p_configClass);
    }

    /**
     * Register a DXRAM service
     *
     * @param p_class
     *         Class of the service to register
     * @param p_configClass
     *         Configuration class to associate with the component
     */
    public void registerService(final Class<? extends AbstractDXRAMService> p_class,
            final Class<? extends DXRAMModuleConfig> p_configClass) {
        if (m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        m_serviceManager.register(p_class, p_configClass);
    }

    /**
     * Create a new configuration instance based on the default values of the registered and available
     * components and services
     *
     * @return New configuration instance
     */
    public DXRAMConfig createConfigInstance() {
        if (m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        return new DXRAMConfig(m_componentManager.createDefaultConfigs(), m_serviceManager.createDefaultConfigs());
    }

    /**
     * Initialize DXRAM with a configuration
     *
     * @param p_config
     *         Configuration for DXRAM
     * @return True if initialization successful, false on error or if a new configuration was generated
     */
    public boolean init(final DXRAMConfig p_config) {
        if (m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        // log final config for debugging
        // LOGGER.debug("Configuration for this instance: %s", p_config);

        LOGGER.debug("Verifying configuration...");

        // verify configuration values
        if (!p_config.verifyConfigurationValuesComponents()) {
            return false;
        }

        if (!p_config.verifyConfigurationValuesServices()) {
            return false;
        }

        m_config = p_config;

        // -----------------------------

        m_jniManager = new DXRAMJNIManager(m_config.getEngineConfig().getJniPath());

        // -----------------------------

        LOGGER.debug("Configuration verification successful");

        LOGGER.debug("Initializing component manager...");
        m_componentManager.init(p_config.getEngineConfig().getRole(), p_config.m_componentConfigs);

        LOGGER.debug("Initializing service manager...");
        m_serviceManager.init(p_config.getEngineConfig().getRole(), p_config.m_serviceConfigs);

        // -----------------------------

        // sort list by initialization priority
        List<AbstractDXRAMComponent> components = m_componentManager.getModules(AbstractDXRAMComponent.class);
        components.sort(Comparator.comparingInt(AbstractDXRAMComponent::getPriorityInit));

        LOGGER.info("Initializing %d components", components.size());

        for (AbstractDXRAMComponent component : components) {
            if (!component.init(this)) {
                LOGGER.error("Initializing component '%s' failed, aborting init", component.getName());
                return false;
            }
        }

        LOGGER.debug("Initializing components done");

        // -----------------------------

        List<AbstractDXRAMService> services = m_serviceManager.getModules(AbstractDXRAMService.class);

        LOGGER.info("Initializing %d services", services.size());

        for (AbstractDXRAMService service : services) {
            if (!service.init(this)) {
                LOGGER.error("Starting service '%s' failed, aborting init", service.getName());
                return false;
            }
        }

        LOGGER.debug("Initializing services done");

        // -----------------------------

        LOGGER.debug("Triggering engineInitFinished on all components... ");

        for (AbstractDXRAMComponent component : components) {
            component.engineInitFinished();
        }

        // -----------------------------

        LOGGER.debug("Triggering engineInitFinished on all services... ");

        for (AbstractDXRAMService service : services) {
            service.engineInitFinished();
        }

        // -----------------------------

        LOGGER.debug("Initializing engine done");

        m_isInitialized = true;

        return true;
    }

    /**
     * Starts the engine.
     */
    public void run() {
        LOGGER.info("Running");
        boolean isRunning = true;
        EngineEvent event;

        m_engineLock.lock();

        while (isRunning) {
            try {
                event = m_events.take();
            } catch (InterruptedException e) {
                continue;
            }

            isRunning = handleEvent(event);
        }

        m_engineLock.unlock();
    }

    /**
     * Handles an event regarding the engine.
     *
     * @param p_event The event to handle.
     * @return True if the engine should continue running; False else.
     */
    private boolean handleEvent(EngineEvent p_event) {
        switch (p_event) {
            case SHUTDOWN:
                return !shutdown();
            case REBOOT:
                return reboot();
            default:
                return false;
        }
    }

    /**
     * Shut down the engine.
     *
     * @return True if successful, false otherwise.
     */
    public boolean shutdown() {
        if (!m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        // -----------------------------

        List<AbstractDXRAMService> services = m_serviceManager.getModules(AbstractDXRAMService.class);

        LOGGER.info("Shutting down %d services", services.size());

        services.stream().filter(service -> !service.shutdown()).forEach(
                service -> LOGGER.error("Shutting down service '%s' failed.", service.getName()));

        LOGGER.debug("Shutting down services done");

        // -----------------------------

        List<AbstractDXRAMComponent> components = m_componentManager.getModules(AbstractDXRAMComponent.class);
        components.sort(Comparator.comparingInt(AbstractDXRAMComponent::getPriorityShutdown));

        LOGGER.info("Shutting down %d components", components.size());

        components.forEach(AbstractDXRAMComponent::shutdown);

        LOGGER.debug("Shutting down components done");
        LOGGER.info("Bye");

        // -----------------------------

        m_isInitialized = false;

        return true;
    }

    private boolean reboot() {
        return shutdown() || init(m_config);
    }

    /**
     * Pushes a new event to the engine.
     *
     * @param p_event The event to push.
     */
    private void pushEvent(final EngineEvent p_event) {
        while (true) {
            try {
                m_events.put(p_event);
            } catch (InterruptedException e) {
                continue;
            }

            break;
        }
    }

    /**
     * Trigger a soft reboot
     */
    public void triggerSoftReboot() {
        pushEvent(EngineEvent.REBOOT);
    }

    public void triggerShutdown() {
        pushEvent(EngineEvent.SHUTDOWN);
    }

    @Override
    public <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
        // don't check for initialized because this call is used to resolve component
        // dependencies during initialization

        return m_componentManager.getModule(p_class);
    }

    @Override
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        if (!m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        return m_serviceManager.getModule(p_class);
    }

    @Override
    public boolean isServiceAvailable(final Class<? extends AbstractDXRAMService> p_class) {
        if (!m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        return m_serviceManager.getModule(p_class) != null;
    }

    /**
     * Get the configuration instance
     *
     * @return Configuration
     */
    DXRAMConfig getConfig() {
        return m_config;
    }

    /**
     * Get the JNI manager instance
     *
     * @return JNI manager
     */
    DXRAMJNIManager getJNIManager() {
        return m_jniManager;
    }
}
