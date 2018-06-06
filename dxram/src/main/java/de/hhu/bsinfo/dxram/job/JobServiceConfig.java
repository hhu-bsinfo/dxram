package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class JobServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public JobServiceConfig() {
        super(JobService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
