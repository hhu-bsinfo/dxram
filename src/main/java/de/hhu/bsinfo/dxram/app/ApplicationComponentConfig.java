package de.hhu.bsinfo.dxram.app;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ApplicationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMComponentConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class ApplicationComponentConfig extends DXRAMComponentConfig {
    @Expose
    private String m_applicationPath = "dxapp";

    /**
     * Path for application jar packages
     */
    public String getApplicationPath() {
        return m_applicationPath;
    }
}
