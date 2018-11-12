package de.hhu.bsinfo.dxram.app;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;

/**
 * Config for the ApplicationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ApplicationServiceConfig extends DXRAMModuleConfig {
    @Data
    @Accessors(prefix = "m_")
    public static class ApplicationEntry {
        /**
         * Fully qualified name of the application class
         */
        @Expose
        private String m_className;

        /**
         * Arguments to pass to the application on start
         */
        @Expose
        private String m_args;

        /**
         * Integer to determine the start order of applications
         */
        @Expose
        private int m_startOrderId = 0;
    }

    /**
     * List of applications to run once DXRAM finished booting
     */
    @Expose
    private List<ApplicationEntry> m_autoStart = Collections.emptyList();

    /**
     * Constructor
     */
    public ApplicationServiceConfig() {
        super(ApplicationService.class);
    }
}
