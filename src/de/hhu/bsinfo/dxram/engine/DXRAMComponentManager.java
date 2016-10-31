
package de.hhu.bsinfo.dxram.engine;

import java.util.HashMap;
import java.util.Map;

/**
 * Manager for all components in DXRAM.
 * All components used in DXRAM must be registered here to create a default configuration with all
 * components listed.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.10.2016
 */
public class DXRAMComponentManager {

    private Map<String, Class<? extends AbstractDXRAMComponent>> m_registeredComponents = new HashMap<>();

    /**
     * Constructor
     */
    DXRAMComponentManager() {

    }

    /**
     * Register a component
     * @param p_class
     *            Component class to register
     */
    public void register(final Class<? extends AbstractDXRAMComponent> p_class) {
        m_registeredComponents.put(p_class.getSimpleName(), p_class);
    }

    /**
     * Create an instance of a component
     * @param p_className
     *            Simple class name (without package path)
     * @return Instance of the component
     */
    AbstractDXRAMComponent createInstance(final String p_className) {

        Class<? extends AbstractDXRAMComponent> clazz = m_registeredComponents.get(p_className);

        try {
            return clazz.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Cannot create component instance of " + clazz.getSimpleName(), e);
        }
    }

    /**
     * Create instances of all registered components
     * @return List of instances of all registered components
     */
    AbstractDXRAMComponent[] createAllInstances() {
        AbstractDXRAMComponent[] instances = new AbstractDXRAMComponent[m_registeredComponents.size()];
        int index = 0;

        for (Class<? extends AbstractDXRAMComponent> clazz : m_registeredComponents.values()) {
            try {
                instances[index++] = clazz.getConstructor().newInstance();
            } catch (final Exception e) {
                throw new RuntimeException("Cannot create component instance of " + clazz.getSimpleName(), e);
            }
        }

        return instances;
    }
}
