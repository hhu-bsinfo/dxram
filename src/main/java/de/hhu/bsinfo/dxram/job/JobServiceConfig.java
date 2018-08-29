package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = JobService.class, supportsSuperpeer = false, supportsPeer = true)
public class JobServiceConfig extends DXRAMServiceConfig {

}
