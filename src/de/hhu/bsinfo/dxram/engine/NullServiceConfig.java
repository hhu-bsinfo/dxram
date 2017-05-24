package de.hhu.bsinfo.dxram.engine;

/**
 * Config for NullService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NullServiceConfig extends DXRAMServiceConfig {
    /**
     * Constructor
     */
    public NullServiceConfig() {
        super(NullService.class, true, true);
    }
}
