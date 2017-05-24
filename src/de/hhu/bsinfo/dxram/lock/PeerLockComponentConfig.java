package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the PeerLockComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class PeerLockComponentConfig extends DXRAMComponentConfig {
    /**
     * Constructor
     */
    public PeerLockComponentConfig() {
        super(PeerLockComponent.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
