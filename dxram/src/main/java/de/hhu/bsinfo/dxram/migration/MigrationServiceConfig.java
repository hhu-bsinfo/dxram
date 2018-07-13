package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the MigrationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class MigrationServiceConfig extends AbstractDXRAMServiceConfig {
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
