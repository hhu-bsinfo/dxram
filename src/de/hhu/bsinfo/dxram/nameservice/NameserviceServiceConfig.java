package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the NameserviceService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NameserviceServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public NameserviceServiceConfig() {
        super(NameserviceService.class, false, true);
    }

    @Override
    protected boolean verify(DXRAMContext.Config p_config) {
        return true;
    }
}
