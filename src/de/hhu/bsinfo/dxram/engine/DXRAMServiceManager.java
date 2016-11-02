package de.hhu.bsinfo.dxram.engine;

import java.util.HashMap;
import java.util.Map;

/**
 * Manager for all services in DXRAM.
 * All services used in DXRAM must be registered here to create a default configuration with all services listed.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.10.2016
 */
class DXRAMServiceManager {

    private Map<String, Class<? extends AbstractDXRAMService>> m_registeredServices = new HashMap<>();

    /**
     * Constructor
     */
    DXRAMServiceManager() {

    }

    /**
     * Register a service
     *
     * @param p_class
     *     Serivce class to register
     */
    public void register(final Class<? extends AbstractDXRAMService> p_class) {
        m_registeredServices.put(p_class.getSimpleName(), p_class);
    }

    /**
     * Create an instance of a service
     *
     * @param p_className
     *     Name of the class (without package path)
     * @return Instance of the service
     */
    AbstractDXRAMService createInstance(final String p_className) {

        Class<? extends AbstractDXRAMService> clazz = m_registeredServices.get(p_className);

        try {
            return clazz.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Cannot create service instance of " + clazz.getSimpleName(), e);
        }
    }

    /**
     * Create instances of all registered services
     *
     * @return List of instances of all registered services
     */
    AbstractDXRAMService[] createAllInstances() {
        AbstractDXRAMService[] instances = new AbstractDXRAMService[m_registeredServices.size()];
        int index = 0;

        for (Class<? extends AbstractDXRAMService> clazz : m_registeredServices.values()) {
            try {
                instances[index++] = clazz.getConstructor().newInstance();
            } catch (final Exception e) {
                throw new RuntimeException("Cannot create component instance of " + clazz.getSimpleName(), e);
            }
        }

        return instances;
    }
}
