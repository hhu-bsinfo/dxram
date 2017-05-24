package de.hhu.bsinfo.dxram.failure;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the FailureComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class FailureComponentConfig extends DXRAMComponentConfig {
    /**
     * Constructor
     */
    public FailureComponentConfig() {
        super(FailureComponent.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
