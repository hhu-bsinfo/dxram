package de.hhu.bsinfo.dxram.engine;

/**
 * Config for NullService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = NullService.class, supportsSuperpeer = true, supportsPeer = true)
public class NullServiceConfig extends DXRAMServiceConfig {

}
