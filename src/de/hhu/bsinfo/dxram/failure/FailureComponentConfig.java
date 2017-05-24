package de.hhu.bsinfo.dxram.failure;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the FailureComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class FailureComponentConfig extends DXRAMComponentConfig {
    /**
     * Constructor
     */
    public FailureComponentConfig() {
        super(FailureComponent.class, true, true);
    }
}
