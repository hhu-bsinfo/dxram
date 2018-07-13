package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ChunkAsyncService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkAsyncServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public ChunkAsyncServiceConfig() {
        super(ChunkAsyncService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
