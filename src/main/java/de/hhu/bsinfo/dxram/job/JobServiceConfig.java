package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class JobServiceConfig extends AbstractDXRAMServiceConfig {
    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
