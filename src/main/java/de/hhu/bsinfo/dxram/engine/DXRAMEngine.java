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

    private final DXRAMVersion m_version;
    private final DXRAMComponentManager m_componentManager;
    private final DXRAMServiceManager m_serviceManager;

    private DXRAMContextCreator m_contextCreator;
    private DXRAMContext m_context;

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
            AbstractDXRAMService tmpService = m_context.getServices().get(p_class.getSimpleName());
            if (tmpService == null) {
                // check for any kind of instance of the specified class
                // we might have another interface/abstract class between the
                // class we request and an instance we could serve
                for (Entry<String, AbstractDXRAMService> entry : m_context.getServices()
                        .entrySet()) {
                    tmpService = entry.getValue();
                    if (p_class.isInstance(tmpService)) {
                        break;
                    }

                    tmpService = null;
                }
            }

            if (p_class.isInstance(tmpService)) {
                service = p_class.cast(tmpService);
            }

            if (service == null) {
                LOGGER.warn("Service not available %s", p_class);
            }
        }

        if (service == null) {
            LOGGER.warn("Service '%s' not available", p_class.getSimpleName());
        }

        return service;
    }

    @Override
    public AbstractDXRAMService getService(final String p_shortName) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_context.getServices().get(m_servicesShortName.get(p_shortName));
        }

        if (service == null) {
            LOGGER.warn("Service '%s' not available", p_shortName);
        }

        return service;
    }

    @Override
    public <T extends AbstractDXRAMService> boolean isServiceAvailable(final Class<T> p_class) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_context.getServices().get(p_class.getSimpleName());
        }

        return service != null;
    }

    @Override
    public boolean isServiceAvailable(final String p_shortName) {
        AbstractDXRAMService service = null;

        if (m_isInitialized) {
            service = m_context.getServices().get(m_servicesShortName.get(p_shortName));
        }

        return service != null;
    }

    @Override
    public <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
        T component = null;

        AbstractDXRAMComponent tmpComponent = m_context.getComponents().get(
                p_class.getSimpleName());
        if (tmpComponent == null) {
            // check for any kind of instance of the specified class
            // we might have another interface/abstract class between the
            // class we request and an instance we could serve
            for (Entry<String, AbstractDXRAMComponent> entry : m_context.getComponents()
                    .entrySet()) {
                tmpComponent = entry.getValue();
                if (p_class.isInstance(tmpComponent)) {
                    break;
                }

                tmpComponent = null;
            }
        }

        if (p_class.isInstance(tmpComponent)) {
            component = p_class.cast(tmpComponent);
        }

        if (component == null) {
            LOGGER.warn("Getting component '%s', not available", p_class.getSimpleName());
        }

        return component;
    }

    /**
     * Initialize DXRAM with a configuration
     *
     * @param p_creator
     *         Context creator which creates a context with configuration
     * @return True if initialization successful, false on error or if a new configuration was generated
     */
    public boolean init(final DXRAMContextCreator p_creator) {
        if (m_isInitialized) {
            throw new IllegalStateException("Already initialized");
        }

        // bootstrapping configuration

        m_contextCreator = p_creator;
        m_context = m_contextCreator.create(m_componentManager, m_serviceManager);

        if (m_context == null) {
            LOGGER.error("Creating context with creator '%s' failed", p_creator.getClass().getSimpleName());
            return false;
        }

        // verify configuration values
        if (!m_context.verifyConfigurationValuesComponents()) {
            return false;
        }

        if (!m_context.verifyConfigurationValuesComponents()) {
            return false;
        }

        // create component/service instances
        m_context.createComponentsFromConfig(m_componentManager, m_context.getConfig().getEngineConfig().getRole());
        m_context.createServicesFromConfig(m_serviceManager, m_context.getConfig().getEngineConfig().getRole());

        // -----------------------------

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        LOGGER.info("Initializing engine (version %s)...", m_version);

        setupJNI();

        // init the short names for the services
        for (Entry<String, AbstractDXRAMService> service : m_context.getServices().entrySet()) {
            m_servicesShortName.put(service.getValue().getShortName(), service.getKey());
        }

        list = new ArrayList<>(m_context.getComponents().values());

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

        LOGGER.info("Initializing %d components...", list.size());

        for (AbstractDXRAMComponent component : list) {
            if (!component.init(this)) {

                LOGGER.error("Initializing component '%s' failed, aborting init", component.getComponentName());

                return false;
            }
        }

        LOGGER.info("Initializing components done");
        LOGGER.info("Starting %d services...", m_context.getServices().size());

        for (AbstractDXRAMService service : m_context.getServices().values()) {
            // check for null -> invalid service
            if (service == null) {
                LOGGER.fatal("Found null object in service list, most likely due to invalid configuration entry");
                return false;
            }

            if (!service.start(this)) {

                LOGGER.error("Starting service '%s' failed, aborting init", service.getServiceName());

                return false;
            }
        }

        LOGGER.info("Starting services done");
        LOGGER.info("Finishing initialization of components");

        for (AbstractDXRAMComponent component : list) {
            if (!component.finishInitComponent()) {

                LOGGER.error("Finishing initialization of component '%s' failed, aborting init",
                        component.getComponentName());

                return false;
            }
        }

        LOGGER.info("Initializing engine done");

        m_isInitialized = true;

        for (AbstractDXRAMService service : m_context.getServices().values()) {
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
        if (!m_isInitialized) {
            throw new IllegalStateException("Not initialized");
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

            if (!init(m_contextCreator)) {
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
            throw new IllegalStateException("Not initialized");
        }

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        LOGGER.info("Shutting down engine...");
        LOGGER.info("Shutting down %d services...", m_context.getServices().size());

        m_context.getServices().values().stream().filter(service -> !service.shutdown()).forEach(
                service -> LOGGER.error("Shutting down service '%s' failed.", service.getServiceName()));

        m_servicesShortName.clear();

        LOGGER.info("Shutting down services done");

        list = new ArrayList<>(m_context.getComponents().values());

        comp = Comparator.comparingInt(AbstractDXRAMComponent::getPriorityShutdown);

        list.sort(comp);

        LOGGER.info("Shutting down %d components...", list.size());

        list.forEach(AbstractDXRAMComponent::shutdown);

        LOGGER.info("Shutting down components done");
        LOGGER.info("Shutting down engine done");

        m_context = null;

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
        return m_context.getConfig();
    }

    /**
     * Setup JNI related stuff.
     */
    private void setupJNI() {
        DXRAMJNIManager.setup(m_context.getConfig().getEngineConfig());
    }
}
