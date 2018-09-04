package de.hhu.bsinfo.dxram.migration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the MigrationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = MigrationService.class, supportsSuperpeer = false, supportsPeer = true)
public class MigrationServiceConfig extends DXRAMServiceConfig {

}
