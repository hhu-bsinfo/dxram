package de.hhu.bsinfo.dxram.failure;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the FailureComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = FailureComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class FailureComponentConfig extends DXRAMComponentConfig {

}
