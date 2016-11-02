package de.hhu.bsinfo.dxram.engine;

/**
 * Interface to access loaded components of the engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public interface DXRAMComponentAccessor {

    /**
     * Get a component from the engine.
     *
     * @param <T>
     *     Type of the component class.
     * @param p_class
     *     Class of the component to get. If the component has different implementations, use the common
     *     interface
     *     or abstract class to get the registered instance.
     * @return Reference to the component if available and enabled, null otherwise or if the engine is not
     * initialized.
     */
    <T extends AbstractDXRAMComponent> T getComponent(Class<T> p_class);
}
