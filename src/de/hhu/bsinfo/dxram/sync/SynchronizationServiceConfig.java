package de.hhu.bsinfo.dxram.sync;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the SynchronizationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class SynchronizationServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public SynchronizationServiceConfig() {
        super(SynchronizationService.class, false, true);
    }
}
