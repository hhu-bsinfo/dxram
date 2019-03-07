package de.hhu.bsinfo.dxram.management;

import io.javalin.Javalin;
import io.javalin.json.JavalinJson;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import com.google.gson.Gson;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.management.endpoints.Members;
import de.hhu.bsinfo.dxram.management.endpoints.Submit;

@AbstractDXRAMModule.Attributes(supportsSuperpeer = true, supportsPeer = false)
public class ManagementService extends AbstractDXRAMService<ManagementServiceConfig> {

    private final Gson m_gson = new Gson();

    private final MBeanServer m_beanServer = ManagementFactory.getPlatformMBeanServer();

    private AbstractBootComponent<ZookeeperBootComponentConfig> m_bootComponent;
    private ChunkComponent m_chunkComponent;

    private DXRAMComponentAccessor m_componentAccessor;

    private Javalin m_server;

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_componentAccessor = p_componentAccessor;
        m_bootComponent = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_chunkComponent = p_componentAccessor.getComponent(ChunkComponent.class);
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
        return true;
    }
}
