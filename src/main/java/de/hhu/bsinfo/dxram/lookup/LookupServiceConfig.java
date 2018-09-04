package de.hhu.bsinfo.dxram.lookup;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the LookupService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = LookupService.class, supportsSuperpeer = true, supportsPeer = true)
public class LookupServiceConfig extends DXRAMServiceConfig {

}
