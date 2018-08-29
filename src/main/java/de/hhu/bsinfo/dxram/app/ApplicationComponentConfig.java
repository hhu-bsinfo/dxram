package de.hhu.bsinfo.dxram.app;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ApplicationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMComponentConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class ApplicationComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private String m_applicationPath = "dxapp";

    /**
     * Path for application jar packages
     */
    public String getApplicationPath() {
        return m_applicationPath;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
