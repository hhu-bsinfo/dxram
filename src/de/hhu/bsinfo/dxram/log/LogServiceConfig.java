package de.hhu.bsinfo.dxram.log;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the LogService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LogServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public LogServiceConfig() {
        super(LogService.class, false, true);
    }
}
