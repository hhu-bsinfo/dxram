package de.hhu.bsinfo.dxram.nameservice;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the NameserviceService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = NameserviceService.class, supportsSuperpeer = false, supportsPeer = true)
public class NameserviceServiceConfig extends DXRAMServiceConfig {

}
