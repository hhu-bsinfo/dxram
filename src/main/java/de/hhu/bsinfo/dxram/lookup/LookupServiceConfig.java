package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the LookupService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMServiceConfig.Settings(supportsSuperpeer = true, supportsPeer = true)
public class LookupServiceConfig extends AbstractDXRAMServiceConfig {
    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
