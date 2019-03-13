package de.hhu.bsinfo.dxram.nameservice;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.ModuleConfig;

/**
 * Config for the NameserviceComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class NameserviceComponentConfig extends ModuleConfig {
    /**
     * Type of name service string converter to use to convert name service keys (available: NAME and INT)
     */
    @Expose
    private String m_type = "NAME";

    /**
     * The maximum number of nameservice entries to cache locally.
     */
    @Expose
    private int m_nameserviceCacheEntries = 1000000;

    /**
     * Constructor
     */
    public NameserviceComponentConfig() {
        super(NameserviceComponent.class);
    }
}
