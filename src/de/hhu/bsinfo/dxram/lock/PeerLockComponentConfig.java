package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

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
}
