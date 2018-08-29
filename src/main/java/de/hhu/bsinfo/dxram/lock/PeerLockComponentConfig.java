package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the PeerLockComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMComponentConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class PeerLockComponentConfig extends DXRAMComponentConfig {

}
