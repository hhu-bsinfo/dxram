package de.hhu.bsinfo.dxram.loader;

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)

        })
public class DistributedLoaderTest {
    @TestInstance(runOnNodeIdx = 1)
    public void initSuperpeer(final DXRAM p_instance) {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest(final DXRAM p_instance) throws InterruptedException{
        Thread.sleep(100);

        LoaderService loaderService = p_instance.getService(LoaderService.class);

        Class test = null;
        try {
            test = loaderService.getClassLoader().loadClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
        }catch (ClassNotFoundException e) {
            Assert.fail("Oups, classloading failed.");
        }

        Assert.assertNotNull(test);

        try {
            loaderService.getClassLoader().loadClass("BestClass");
            Assert.fail("This class should not exist.");
        }catch (ClassNotFoundException e) {
            // this is nice
        }

    }
}
