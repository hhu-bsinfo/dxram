package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import lombok.Getter;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderService extends Service<ModuleConfig> {
    @Dependency
    private LoaderComponent m_loader;

    public DistributedLoader getClassLoader() {
        return m_loader.getM_loader();
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
