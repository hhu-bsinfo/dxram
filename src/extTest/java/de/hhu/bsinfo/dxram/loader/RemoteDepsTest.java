package de.hhu.bsinfo.dxram.loader;

import java.nio.file.Paths;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)

        })
public class RemoteDepsTest {
    @TestInstance(runOnNodeIdx = 1)
    public void initSuperpeer(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        Thread.sleep(100);

        ApplicationService applicationService = p_instance.getService(ApplicationService.class);
        applicationService.registerApplicationClass(ExternalDepsApp.class);
        applicationService.startApplication("de.hhu.bsinfo.dxram.loader.ExternalDepsApp");
        //applicationService.startApplication("de.hhu.bsinfo.dxapp.HelloApplication");
    }
}
