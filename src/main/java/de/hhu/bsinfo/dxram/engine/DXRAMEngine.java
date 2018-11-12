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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Engine class running DXRAM with components and services.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DXRAMEngine implements DXRAMServiceAccessor, DXRAMComponentAccessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMEngine.class.getSimpleName());

    private final DXRAMVersion m_version;
    private final DXRAMModuleManager m_componentManager;
    private final DXRAMModuleManager m_serviceManager;

    private boolean m_isInitialized;
    private DXRAMConfig m_config;
    private DXRAMJNIManager m_jniManager;

    private volatile boolean m_triggerReboot;

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
        LOGGER.info("Initializing engine (version %s)...", m_version);

        if (m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        // log final config for debugging
        LOGGER.debug("Configuration for this instance: %s", p_config);

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

        LOGGER.info("Initializing %d components...", components.size());

        for (AbstractDXRAMComponent component : components) {
            if (!component.init(this)) {
                LOGGER.error("Initializing component '%s' failed, aborting init", component.getName());
                return false;
            }
        }

        LOGGER.info("Initializing components done");

        // -----------------------------

        List<AbstractDXRAMService> services = m_serviceManager.getModules(AbstractDXRAMService.class);

        LOGGER.info("Starting %d services...", services.size());

        for (AbstractDXRAMService service : services) {
            if (!service.init(this)) {
                LOGGER.error("Starting service '%s' failed, aborting init", service.getName());
                return false;
            }
        }

        LOGGER.info("Starting services done");

        // -----------------------------

        LOGGER.info("Triggering engineInitFinished on all components... ");

        for (AbstractDXRAMComponent component : components) {
            component.engineInitFinished();
        }

        // -----------------------------

        LOGGER.info("Initializing engine done");

        m_isInitialized = true;

        return true;
    }

    /**
     * The engine must be driven by the main thread
     *
     * @return True if update successful, false on error
     */
    public boolean update() {
        if (!m_isInitialized) {
            throw new IllegalStateException("Invalid initialization state");
        }

        if (Thread.currentThread().getId() != 1) {
            throw new RuntimeException(
                    "Update called by thread-" + Thread.currentThread().getId() + " (" +
                            Thread.currentThread().getName() + "), not main thread");
        }

        if (m_triggerReboot) {
            LOGGER.info("Executing instant soft reboot");

            if (!shutdown()) {
                return false;
            }

            if (!init(m_config)) {
                return false;
            }

            m_triggerReboot = false;
        }

        try {
            Thread.sleep(1000);
        } catch (final InterruptedException ignored) {
        }

        return true;
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

        LOGGER.info("Shutting down engine...");

        // -----------------------------

        List<AbstractDXRAMService> services = m_serviceManager.getModules(AbstractDXRAMService.class);

        LOGGER.info("Shutting down %d services...", services.size());

        services.stream().filter(service -> !service.shutdown()).forEach(
                service -> LOGGER.error("Shutting down service '%s' failed.", service.getName()));

        LOGGER.info("Shutting down services done");

        // -----------------------------

        List<AbstractDXRAMComponent> components = m_componentManager.getModules(AbstractDXRAMComponent.class);
        components.sort(Comparator.comparingInt(AbstractDXRAMComponent::getPriorityShutdown));

        LOGGER.info("Shutting down %d components...", components.size());

        components.forEach(AbstractDXRAMComponent::shutdown);

        LOGGER.info("Shutting down components done");

        // -----------------------------

        LOGGER.info("Shutting down engine done");

        m_isInitialized = false;

        return true;
    }

    /**
     * Trigger a soft reboot on the next update cycle
     */
    public void triggerSoftReboot() {
        m_triggerReboot = true;
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
