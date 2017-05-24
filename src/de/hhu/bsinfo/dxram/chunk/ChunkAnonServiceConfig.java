package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkAnonService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkAnonServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public ChunkAnonServiceConfig() {
        super(ChunkAnonService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
