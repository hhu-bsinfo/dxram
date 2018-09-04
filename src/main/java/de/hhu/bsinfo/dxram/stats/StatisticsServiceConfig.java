package de.hhu.bsinfo.dxram.stats;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the StatisticsService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = StatisticsService.class, supportsSuperpeer = true, supportsPeer = true)
public class StatisticsServiceConfig extends DXRAMServiceConfig {
    /**
     * If non zero, enables a dedicated thread that prints the statistics periodically.
     */
    @Expose
    private int m_printStatsPeriodMs = 0;
}
