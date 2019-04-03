package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import sun.net.spi.nameservice.NameService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DistributedLoaderTest {
    boolean node1finished = false;

    @TestInstance(runOnNodeIdx = 0)
    public void initSuperpeer(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.registerJar(Paths.get("dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        Thread.yield();
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.getClassLoader().loadClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
        loaderService.cleanLoaderDir();
    }
}
