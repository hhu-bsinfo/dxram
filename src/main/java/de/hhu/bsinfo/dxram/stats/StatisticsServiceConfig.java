package de.hhu.bsinfo.dxram.stats;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class StatisticsServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public StatisticsServiceConfig() {
        super(StatisticsService.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
