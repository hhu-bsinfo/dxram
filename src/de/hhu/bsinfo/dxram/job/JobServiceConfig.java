package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class JobServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public JobServiceConfig() {
        super(JobService.class, false, true);
    }
}
