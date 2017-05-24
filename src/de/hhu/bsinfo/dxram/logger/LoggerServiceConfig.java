package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the LoggerService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LoggerServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public LoggerServiceConfig() {
        super(LoggerService.class, true, true);
    }
}
