package de.hhu.bsinfo.dxram.stats;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class StatisticsServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public StatisticsServiceConfig() {
        super(StatisticsService.class, true, true);
    }
}
