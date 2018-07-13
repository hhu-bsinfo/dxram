package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ChunkDebugService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkDebugServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public ChunkDebugServiceConfig() {
        super(ChunkDebugService.class, false, false);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
