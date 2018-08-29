package de.hhu.bsinfo.dxram.app;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ApplicationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = ApplicationService.class, supportsSuperpeer = false, supportsPeer = true)
public class ApplicationServiceConfig extends DXRAMServiceConfig {

}
