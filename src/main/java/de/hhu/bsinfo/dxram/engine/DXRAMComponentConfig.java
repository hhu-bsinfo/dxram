package de.hhu.bsinfo.dxram.engine;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.annotation.Annotation;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides configuration values for a component. Use this as a base class for all components to add further
 * configuration values
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Accessors(prefix = "m_")
public class DXRAMComponentConfig {
    protected final Logger LOGGER;

    /**
     * Get the class name of the component of this configuration
     */
    @Expose
    @Getter
    private String m_componentClassName;

    /**
     * True if component supports the superpeer node role, false otherwise
     */
    @Expose
    @Getter
    private boolean m_supportsSuperpeer = false;

    /**
     * True if component supports the peer node role, false otherwise
     */
    @Expose
    @Getter
    private boolean m_supportsPeer = false;

    /**
     * Constructor
     */
    protected DXRAMComponentConfig() {
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());
        m_componentClassName = getClass().getSimpleName();

        Annotation[] annotations = getClass().getAnnotations();

        for (Annotation annotation : annotations) {
            if (annotation instanceof Settings) {
                Settings ann = (Settings) annotation;

                m_supportsSuperpeer = ann.supportsPeer();
                m_supportsPeer = ann.supportsPeer();
            }
        }

        if (!m_supportsSuperpeer && !m_supportsPeer) {
            throw new IllegalStateException("Component " + m_componentClassName +
                    " has to support at least one node role");
        }
    }

    /**
     * Verify the configuration values: Check limits, validate strings, ...
     *
     * @param p_config
     *         Full configuration to access other config values on dependencies
     * @return True if verification successful, false on error
     */
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }

    /**
     * Settings for component config
     */
    public @interface Settings {
        /**
         * True if component supports the superpeer node role, false otherwise
         */
        boolean supportsSuperpeer();

        /**
         * True if component supports the peer node role, false otherwise
         */
        boolean supportsPeer();
    }
}
