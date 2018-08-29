package de.hhu.bsinfo.dxram.stats;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = StatisticsService.class, supportsSuperpeer = true, supportsPeer = true)
public class StatisticsServiceConfig extends DXRAMServiceConfig {
    @Expose
    private int m_printStatsPeriodMs = 0;

    /**
     * If non zero, enables a dedicated thread that prints the statistics periodically.
     *
     * @return Print interval in ms
     */
    public int getPrintStatsPeriodMs() {
        return m_printStatsPeriodMs;
    }
}
