package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the NetworkService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NetworkServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public NetworkServiceConfig() {
        super(NetworkService.class, true, true);
    }
}
