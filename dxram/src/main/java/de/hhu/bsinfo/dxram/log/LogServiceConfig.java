package de.hhu.bsinfo.dxram.log;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the LogService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LogServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public LogServiceConfig() {
        super(LogService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
