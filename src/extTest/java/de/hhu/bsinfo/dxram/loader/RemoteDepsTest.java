package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.runner.RunWith;

import java.nio.file.Paths;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class RemoteDepsTest {
    @TestInstance(runOnNodeIdx = 0)
    public void initSuperpeer(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        Thread.yield();

        ApplicationService applicationService = p_instance.getService(ApplicationService.class);
        applicationService.startApplication("de.hhu.bsinfo.dxapp.HelloApplication");
    }
}
