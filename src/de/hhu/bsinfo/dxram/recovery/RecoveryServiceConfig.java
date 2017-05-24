package de.hhu.bsinfo.dxram.recovery;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the RecoveryService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class RecoveryServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public RecoveryServiceConfig() {
        super(RecoveryService.class, false, true);
    }
}
