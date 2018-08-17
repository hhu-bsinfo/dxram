package de.hhu.bsinfo.dxram.engine;

/**
 * Interface for classes creating a DXRAM context from a configuration (e.g. loading from file)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.08.2017
 */
public abstract class AbstractDXRAMContextCreator {
    /**
     * Create a DXRAMContext instance
     *
     * @param p_componentManager
     *         Component manager with registered components
     * @param p_serviceManager
     *         Service manager with registered services
     * @return DXRAMContext instance
     */
    DXRAMContext create(final DXRAMComponentManager p_componentManager, final DXRAMServiceManager p_serviceManager);
}
