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

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main class to run DXRAM with components and services.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DXRAMEngine implements DXRAMServiceAccessor, DXRAMComponentAccessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMEngine.class.getSimpleName());

    private DXRAMVersion m_version;
    private DXRAMComponentManager m_componentManager;
    private DXRAMServiceManager m_serviceManager;
    private DXRAMContextHandler m_contextHandler;

    private boolean m_isInitialized;
    private volatile boolean m_triggerReboot;

    private Map<String, String> m_servicesShortName = new HashMap<>();

    /**
     * Constructor
     *
     * @param p_version
     *         Object to label the current version
     */
    public DXRAMEngine(final DXRAMVersion p_version) {
        m_version = Objects.requireNonNull(p_version);
        m_componentManager = new DXRAMComponentManager();
        m_serviceManager = new DXRAMServiceManager();
    }

    /**
     * Get the version of the current DXRAM (engine) instance
     *
     * @return Version of current DXRAM instance
     */
    public DXRAMVersion getVersion() {
        return m_version;
    }

    @Override
    public List<String> getServiceShortNames() {
        return new ArrayList<>(m_servicesShortName.keySet());
    }

    /**
     * Register a DXRAM component
     *
     * @param p_class
     *         Class of the component to register
     */
    public void registerComponent(final Class<? extends AbstractDXRAMComponent> p_class) {
        m_componentManager.register(p_class);
    }

    /**
     * Register a DXRAM service
     *
     * @param p_class
     *         Class of the service to register
     */
    public void registerService(final Class<? extends AbstractDXRAMService> p_class) {
        m_serviceManager.register(p_class);
    }

    @Override
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        T service = null;

        if (m_isInitialized) {
            AbstractDXRAMService tmpService = m_contextHandler.getContext().getServices().get(p_class.getSimpleName());
            if (tmpService == null) {
                // check for any kind of instance of the specified class
                // we might have another interface/abstract class between the
                // class we request and an instance we could serve
                for (Entry<String, AbstractDXRAMService> entry : m_contextHandler.getContext().getServices().entrySet()) {
                    tmpService = entry.getValue();
                    if (p_class.isInstance(tmpService)) {
                        break;
                    }

                    tmpService = null;
                }
            }

            if (tmpService != null && p_class.isInstance(tmpService)) {
                service = p_class.cast(tmpService);
            }

            // #if LOGGER >= WARN
            if (service == null) {
                LOGGER.warn("Service not available %s", p_class);
            }
            // #endif /* LOGGER >= WARN */
        }

        // #if LOGGER >= WARN
        if (service == null) {
            LOGGER.warn("Service '%s' not available", p_class.getSimpleName());
        }
        // #endif /* LOGGER >= WARN */

        return service;
    }

    @Override
    public AbstractDXRAMService getService(final String p_shortName) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_contextHandler.getContext().getServices().get(m_servicesShortName.get(p_shortName));
        }

        // #if LOGGER >= WARN
        if (service == null) {
            LOGGER.warn("Service '%s' not available", p_shortName);
        }
        // #endif /* LOGGER >= WARN */

        return service;
    }

    @Override
    public <T extends AbstractDXRAMService> boolean isServiceAvailable(final Class<T> p_class) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_contextHandler.getContext().getServices().get(p_class.getSimpleName());
        }

        return service != null;
    }

    @Override
    public boolean isServiceAvailable(final String p_shortName) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_contextHandler.getContext().getServices().get(m_servicesShortName.get(p_shortName));
        }

        return service != null;
    }

    @Override
    public <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
        T component = null;

        AbstractDXRAMComponent tmpComponent = m_contextHandler.getContext().getComponents().get(p_class.getSimpleName());
        if (tmpComponent == null) {
            // check for any kind of instance of the specified class
            // we might have another interface/abstract class between the
            // class we request and an instance we could serve
            for (Entry<String, AbstractDXRAMComponent> entry : m_contextHandler.getContext().getComponents().entrySet()) {
                tmpComponent = entry.getValue();
                if (p_class.isInstance(tmpComponent)) {
                    break;
                }

                tmpComponent = null;
            }
        }

        if (tmpComponent != null && p_class.isInstance(tmpComponent)) {
            component = p_class.cast(tmpComponent);
        }

        // #if LOGGER >= WARN
        if (component == null) {
            LOGGER.warn("Getting component '%s', not available", p_class.getSimpleName());
        }
        // #endif /* LOGGER >= WARN */

        return component;
    }

    /**
     * Initialize DXRAM with a configuration file
     *
     * @return True if initialization successful, false on error or if a new configuration was generated
     */
    public boolean init() {
        assert !m_isInitialized;

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        // #if LOGGER >= INFO
        LOGGER.info("Initializing engine (version %s)...", m_version);
        // #endif /* LOGGER >= INFO */

        if (!bootstrap()) {
            // false indicates here that a configuration file was created
            return false;
        }

        // init the short names for the services
        for (Entry<String, AbstractDXRAMService> service : m_contextHandler.getContext().getServices().entrySet()) {
            m_servicesShortName.put(service.getValue().getShortName(), service.getKey());
        }

        list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());

        // check list for null objects -> invalid component in list
        for (AbstractDXRAMComponent component : list) {
            if (component == null) {
                LOGGER.fatal("Found null object in component list, most likely due to invalid configuration entry");
                return false;
            }
        }

        // sort list by initialization priority
        comp = Comparator.comparingInt(AbstractDXRAMComponent::getPriorityInit);
        list.sort(comp);

        // #if LOGGER >= INFO
        LOGGER.info("Initializing %d components...", list.size());
        // #endif /* LOGGER >= INFO */
        for (AbstractDXRAMComponent component : list) {
            if (!component.init(this)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Initializing component '%s' failed, aborting init", component.getComponentName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }
        // #if LOGGER >= INFO
        LOGGER.info("Initializing components done");
        //
        LOGGER.info("Starting %d services...", m_contextHandler.getContext().getServices().size());
        // #endif /* LOGGER >= INFO */

        for (AbstractDXRAMService service : m_contextHandler.getContext().getServices().values()) {
            // check for null -> invalid service
            if (service == null) {
                LOGGER.fatal("Found null object in service list, most likely due to invalid configuration entry");
                return false;
            }

            if (!service.start(this)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Starting service '%s' failed, aborting init", service.getServiceName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }
        // #if LOGGER >= INFO
        LOGGER.info("Starting services done");
        //
        LOGGER.info("Finishing initialization of components");
        // #endif /* LOGGER >= INFO */

        for (AbstractDXRAMComponent component : list) {
            if (!component.finishInitComponent()) {
                // #if LOGGER >= ERROR
                LOGGER.error("Finishing initialization of component '%s' failed, aborting init", component.getComponentName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }

        // #if LOGGER >= INFO
        LOGGER.info("Initializing engine done");
        // #endif /* LOGGER >= INFO */

        m_isInitialized = true;

        for (AbstractDXRAMService service : m_contextHandler.getContext().getServices().values()) {
            service.engineInitFinished();
        }

        return true;
    }

    /**
     * The engine must be driven by the main thread
     *
     * @return True if update successful, false on error
     */
    public boolean update() {
        if (Thread.currentThread().getId() != 1) {
            throw new RuntimeException(
                    "Update called by thread-" + Thread.currentThread().getId() + " (" + Thread.currentThread().getName() + "), not main thread");
        }

        if (m_triggerReboot) {
            LOGGER.info("Executing instant soft reboot");
            if (!shutdown()) {
                return false;
            }

            if (!init()) {
                return false;
            }

            m_triggerReboot = false;
        }

        // avoid cpu banging
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
        assert m_isInitialized;

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down engine...");
        // #endif /* LOGGER >= INFO */
        // #if LOGGER >= INFO
        LOGGER.info("Shutting down %d services...", m_contextHandler.getContext().getServices().size());
        // #endif /* LOGGER >= INFO */

        m_contextHandler.getContext().getServices().values().stream().filter(service -> !service.shutdown()).forEach(service -> {
            // #if LOGGER >= ERROR
            LOGGER.error("Shutting down service '%s' failed.", service.getServiceName());
            // #endif /* LOGGER >= ERROR */
        });
        m_servicesShortName.clear();

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down services done");
        // #endif /* LOGGER >= INFO */

        list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());

        comp = Comparator.comparingInt(AbstractDXRAMComponent::getPriorityShutdown);

        list.sort(comp);

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down %d components...", list.size());
        // #endif /* LOGGER >= INFO */

        list.forEach(AbstractDXRAMComponent::shutdown);

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down components done");
        // #endif /* LOGGER >= INFO */
        // #endif /* LOGGER >= INFO */
        LOGGER.info("Shutting down engine done");
        // #endif /* LOGGER >= INFO */

        m_contextHandler = null;

        m_isInitialized = false;

        return true;
    }

    /**
     * Trigger a soft reboot on the next update cycle
     */
    public void triggerSoftReboot() {
        m_triggerReboot = true;
    }

    /**
     * Get the configuration instance
     *
     * @return Configuration
     */
    DXRAMContext.Config getConfig() {
        return m_contextHandler.getContext().getConfig();
    }

    /**
     * Execute bootstrapping tasks for the engine.
     *
     * @return false if a configuration file had to be created, true if not
     */
    private boolean bootstrap() {
        m_contextHandler = new DXRAMContextHandler(m_componentManager, m_serviceManager);

        // check vm arguments for configuration override
        String config = System.getProperty("dxram.config");
        if (config == null) {
            config = "";
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Loading configuration file: %s", config);
            // #endif /* LOGGER >= INFO */
        }

        // check if a config needs to be created
        if (config.isEmpty() || !new File(config).exists()) {
            m_contextHandler.createDefaultConfiguration(config);
            // #if LOGGER >= INFO
            LOGGER.info("Default configuration created (%s), please restart DXRAM", config);
            // #endif /* LOGGER >= INFO */
            return false;
        }

        // load existing configuration
        if (!m_contextHandler.loadConfiguration(config)) {
            // #if LOGGER >= ERROR
            LOGGER.info("Loading configuration failed: %s", config);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        setupJNI();

        return true;
    }

    /**
     * Setup JNI related stuff.
     */
    private void setupJNI() {
        DXRAMJNIManager.setup(m_contextHandler.getContext().getConfig().getEngineConfig());
    }
}
