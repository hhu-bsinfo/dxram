package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;

/**
 * Dummy component implementation for testing.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NullComponent extends AbstractDXRAMComponent {

    /**
     * Constructor
     */
    public NullComponent() {
        super(DXRAMComponentOrder.Init.NULL, DXRAMComponentOrder.Shutdown.NULL);
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }
}
