package de.hhu.bsinfo.dxram.stats;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class StatisticsServiceConfig extends AbstractDXRAMServiceConfig {
    @Expose
    private int m_printStatsPeriodMs = 0;

    /**
     * Constructor
     */
    public StatisticsServiceConfig() {
        super(StatisticsService.class, true, true);
    }

    /**
     * If non zero, enables a dedicated thread that prints the statistics periodically.
     *
     * @return Print interval in ms
     */
    public int getPrintStatsPeriodMs() {
        return m_printStatsPeriodMs;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
