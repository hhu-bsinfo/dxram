package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the MigrationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class MigrationServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public MigrationServiceConfig() {
        super(MigrationService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
