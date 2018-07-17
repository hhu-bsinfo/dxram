package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the LoggerService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LoggerServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public LoggerServiceConfig() {
        super(LoggerService.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
