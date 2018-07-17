package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the NameserviceService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NameserviceServiceConfig extends AbstractDXRAMServiceConfig {
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
