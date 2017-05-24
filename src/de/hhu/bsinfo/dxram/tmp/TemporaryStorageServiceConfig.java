package de.hhu.bsinfo.dxram.tmp;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the TemporaryStorageService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TemporaryStorageServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public TemporaryStorageServiceConfig() {
        super(TemporaryStorageService.class, false, true);
    }
}
