package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the NameserviceService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = NameserviceService.class, supportsSuperpeer = false, supportsPeer = true)
public class NameserviceServiceConfig extends DXRAMServiceConfig {

}
