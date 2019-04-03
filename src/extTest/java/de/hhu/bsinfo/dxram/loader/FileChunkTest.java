package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.NodeID;
import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;

import java.io.File;
import java.io.IOException;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class FileChunkTest {
    long fileChunkId = 0;

    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        BootService bootService = p_instance.getService(BootService.class);

        File testFile = new File("test");

        if(!testFile.exists()){
            try{
                testFile.createNewFile();
            }catch(IOException e){
                e.printStackTrace();
            }
        }

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .findFirst().orElse(NodeID.INVALID_ID);

        FileChunk fileChunk = new FileChunk(testFile);
        chunkService.create().create(peer, fileChunk);
        chunkService.put().put(fileChunk);

        fileChunkId = fileChunk.getID();
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest2(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        while(fileChunkId == 0){
            Thread.yield();
        }

        FileChunk fileChunk = new FileChunk();
        Assert.assertTrue(fileChunk.getM_name().equals(""));
        fileChunk.setID(fileChunkId);
        Assert.assertTrue(chunkService.get().get(fileChunk));

        Assert.assertTrue(fileChunk.getM_name().equals("test"));
    }
}
