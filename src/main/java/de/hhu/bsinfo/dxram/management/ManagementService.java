package de.hhu.bsinfo.dxram.management;

import io.javalin.Javalin;
import io.javalin.json.JavalinJson;

import com.google.gson.Gson;

import de.hhu.bsinfo.dxram.app.ApplicationComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.management.endpoints.Members;
import de.hhu.bsinfo.dxram.management.endpoints.Submit;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

@Module.Attributes(supportsSuperpeer = true, supportsPeer = false)
public class ManagementService extends Service<ManagementServiceConfig> {

    private final Gson m_gson = new Gson();

    @Dependency
    private BootComponent<ZookeeperBootComponentConfig> m_bootComponent;

    @Dependency
    private ApplicationComponent m_applicationComponent;

    @Dependency
    private NetworkComponent m_networkComponent;

    private Javalin m_server;

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        if (!m_bootComponent.getConfig().isBootstrap()) {
            return true;
        }

        m_server = Javalin.create();

        JavalinJson.setFromJsonMapper(m_gson::fromJson);
        JavalinJson.setToJsonMapper(m_gson::toJson);

        m_server.routes(new Members(m_bootComponent));
        m_server.routes(new Submit(m_bootComponent, m_networkComponent));

        m_server.start();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        if (!m_bootComponent.getConfig().isBootstrap()) {
            return true;
        }

        m_server.stop();
        return true;
    }
}
