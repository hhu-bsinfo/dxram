package de.hhu.bsinfo.dxram.engine;

/**
 * Context creator which creates a context with default configuration (temporary, only)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.08.2017
 */
public class DXRAMContextCreatorDefault implements DXRAMContextCreator {
    @Override
    public DXRAMContext create(final DXRAMComponentManager p_componentManager,
            final DXRAMServiceManager p_serviceManager) {
        DXRAMContext context = new DXRAMContext();
        context.createDefaultComponents(p_componentManager);
        context.createDefaultServices(p_serviceManager);
        return context;
    }
}
