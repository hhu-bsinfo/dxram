package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the MigrationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class MigrationServiceConfig extends DXRAMServiceConfig {

}
