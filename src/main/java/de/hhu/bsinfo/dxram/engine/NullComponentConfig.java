package de.hhu.bsinfo.dxram.engine;

/**
 * Config for NullComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMComponentConfig.Settings(supportsSuperpeer = true, supportsPeer = true)
public class NullComponentConfig extends AbstractDXRAMComponentConfig {
    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
