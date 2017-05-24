package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the LookupService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LookupServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public LookupServiceConfig() {
        super(LookupService.class, true, true);
    }
}
