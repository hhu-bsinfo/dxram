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
import java.nio.file.Paths;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DistributedLoaderTest {
    boolean node1finished = false;

    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        BootService bootService = p_instance.getService(BootService.class);
        NameserviceService nameserviceService = p_instance.getService(NameserviceService.class);

        File testFile = new File("dxrest.jar");

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .findFirst().orElse(NodeID.INVALID_ID);

        FileChunk fileChunk = new FileChunk(testFile);
        chunkService.create().create(peer, fileChunk);
        chunkService.put().put(fileChunk);

        nameserviceService.register(fileChunk.getID(), "c1");

        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.getClassLoader().loadClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");

        node1finished = true;
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest2(final DXRAM p_instance) {
        while(!node1finished) {
            Thread.yield();
        }
    }
}
