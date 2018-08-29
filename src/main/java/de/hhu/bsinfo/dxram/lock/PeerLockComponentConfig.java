package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the PeerLockComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMComponentConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class PeerLockComponentConfig extends AbstractDXRAMComponentConfig {
    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
