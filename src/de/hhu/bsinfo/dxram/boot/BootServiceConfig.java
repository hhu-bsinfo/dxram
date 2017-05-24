package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for BootService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class BootServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public BootServiceConfig() {
        super(BootService.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
