package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the NetworkService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NetworkServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public NetworkServiceConfig() {
        super(NetworkService.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
