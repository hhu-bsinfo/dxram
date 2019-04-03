package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.operation.Get;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import lombok.Getter;

import java.nio.file.Paths;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderComponent extends Component<LoaderComponentConfig> {
    @Dependency
    private ChunkComponent m_chunk;
    @Dependency
    private NameserviceComponent m_name;

    @Getter
    private DistributedLoader m_loader;


    @Override
    protected boolean initComponent(DXRAMConfig p_config, DXRAMJNIManager p_jniManager) {
        if (p_config.getEngineConfig().getRole().equals(NodeRole.PEER)) {
            m_loader = new DistributedLoader(Paths.get("loaderTest"), m_chunk, m_name);
        }else{

        }
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }
}
