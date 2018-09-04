package de.hhu.bsinfo.dxram.engine;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * Config for NullService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = NullService.class, supportsSuperpeer = true, supportsPeer = true)
public class NullServiceConfig extends DXRAMServiceConfig {

}
