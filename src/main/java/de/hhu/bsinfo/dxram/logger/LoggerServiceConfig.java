package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the LoggerService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = LoggerService.class, supportsSuperpeer = true, supportsPeer = true)
public class LoggerServiceConfig extends DXRAMServiceConfig {

}
