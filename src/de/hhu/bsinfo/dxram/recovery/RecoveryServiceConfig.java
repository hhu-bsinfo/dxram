package de.hhu.bsinfo.dxram.recovery;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;

/**
 * Config for the RecoveryService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class RecoveryServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public RecoveryServiceConfig() {
        super(RecoveryService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        // TODO kevin
        return true;
    }
}
