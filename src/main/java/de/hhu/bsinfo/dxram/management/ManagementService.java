package de.hhu.bsinfo.dxram.management;

import io.javalin.Javalin;
import io.javalin.json.JavalinJson;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import com.google.gson.Gson;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.Inject;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.management.endpoints.Members;
import de.hhu.bsinfo.dxram.management.endpoints.Submit;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = false)
public class ManagementService extends Service<ManagementServiceConfig> {

    private final Gson m_gson = new Gson();

    @Inject
    private BootComponent<ZookeeperBootComponentConfig> m_bootComponent;

    private ComponentProvider m_componentAccessor;

    private Javalin m_server;

    @Override
    protected void resolveComponentDependencies(ComponentProvider p_componentAccessor) {
        m_componentAccessor = p_componentAccessor;
        m_bootComponent = p_componentAccessor.getComponent(BootComponent.class);
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        if (!m_bootComponent.getConfig().isBootstrap()) {
            return true;
        }

        m_server = Javalin.create();

        JavalinJson.setFromJsonMapper(m_gson::fromJson);
        JavalinJson.setToJsonMapper(m_gson::toJson);

        m_server.routes(new Members(m_bootComponent));
        m_server.routes(new Submit(m_componentAccessor));

        m_server.start();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_server.stop();
        return true;
    }
}
