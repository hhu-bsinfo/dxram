package de.hhu.bsinfo.dxram.stats;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class StatisticsServiceConfig extends DXRAMModuleConfig {
    /**
     * If non zero, enables a dedicated thread that prints the statistics periodically.
     */
    @Expose
    private int m_printStatsPeriodMs = 0;

    /**
     * Constructor
     */
    public StatisticsServiceConfig() {
        super(StatisticsService.class);
    }
}
