package de.hhu.bsinfo.dxram.stats;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Service for internal statistics
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class StatisticsService extends AbstractDXRAMService {
    /**
     * Constructor
     */
    public StatisticsService() {
        super("stats");
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
