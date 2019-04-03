package de.hhu.bsinfo.dxram.loader;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;

public class LoaderComponentConfig extends ModuleConfig {
    /**
     * Get the core configuration values
     */
    @Expose
    private CoreConfig m_coreConfig = new CoreConfig();

    public LoaderComponentConfig() {
        super(LoaderComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        return m_coreConfig.verify();
    }
}
