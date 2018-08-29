package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ChunkAnonService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class ChunkAnonServiceConfig extends AbstractDXRAMServiceConfig {
    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
