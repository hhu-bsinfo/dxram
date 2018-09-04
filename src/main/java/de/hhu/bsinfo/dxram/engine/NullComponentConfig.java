package de.hhu.bsinfo.dxram.engine;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * Config for NullComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = NullComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class NullComponentConfig extends DXRAMComponentConfig {

}
