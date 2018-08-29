package de.hhu.bsinfo.dxram.recovery;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the RecoveryService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = RecoveryService.class, supportsSuperpeer = false, supportsPeer = true)
public class RecoveryServiceConfig extends DXRAMServiceConfig {

}
