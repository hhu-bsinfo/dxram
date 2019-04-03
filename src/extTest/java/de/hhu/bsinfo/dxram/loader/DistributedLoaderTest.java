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
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)

        })
public class DistributedLoaderTest {
    @TestInstance(runOnNodeIdx = 1)
    public void initSuperpeer(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        Thread.sleep(100);

        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.getClassLoader().loadClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
        loaderService.cleanLoaderDir();
    }
}
