package de.hhu.bsinfo.dxram.nameservice;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the NameserviceComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NameserviceComponentConfig extends DXRAMComponentConfig {
    @Expose
    private String m_type = "NAME";

    @Expose
    private int m_nameserviceCacheEntries = 1000000;

    /**
     * Constructor
     */
    public NameserviceComponentConfig() {
        super(NameserviceComponent.class, false, true);
    }

    /**
     * Type of name service string converter to use to convert name service keys (available: NAME and INT)
     */
    public String getType() {
        return m_type;
    }

    // TODO kevin: doc
    public int getNameserviceCacheEntries() {
        return m_nameserviceCacheEntries;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        // TODO kevin
        return true;
    }
}
