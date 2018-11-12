package de.hhu.bsinfo.dxram.app;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;

/**
 * Config for the ApplicationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ApplicationComponentConfig extends DXRAMModuleConfig {
    /**
     * Path to scan for application jar files
     */
    @Expose
    private String m_applicationPath = "dxapp";

    /**
     * Constructor
     */
    public ApplicationComponentConfig() {
        super(ApplicationComponent.class);
    }
}
