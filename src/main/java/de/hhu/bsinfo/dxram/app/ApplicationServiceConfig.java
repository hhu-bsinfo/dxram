package de.hhu.bsinfo.dxram.app;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Config for the ApplicationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = ApplicationService.class, supportsSuperpeer = false, supportsPeer = true)
public class ApplicationServiceConfig extends DXRAMServiceConfig {

    @Expose
    private List<String> m_autoStart = Arrays.asList("TerminalServer");

    public boolean isAutostartEnabled(String p_name) {
        return m_autoStart.contains(p_name);
    }
}
