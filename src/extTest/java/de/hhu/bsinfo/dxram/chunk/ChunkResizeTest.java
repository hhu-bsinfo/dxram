package de.hhu.bsinfo.dxram.chunk;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkResizeTest {
    @TestInstance(runOnNodeIdx = 2)
    public void test(final DXRAM p_instance) {
        NameserviceService nameService = p_instance.getService(NameserviceService.class);
        BootService bootService = p_instance.getService(BootService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);

        TestStrChunk t1 = new TestStrChunk();
        chunkLocalService.createLocal().create(t1);
        chunkService.put().put(t1);

        t1.setAbc("123123");
        chunkService.resize().resize(t1);

        TestStrChunk t3 = new TestStrChunk();
        chunkLocalService.createLocal().create(t3);
    }
}
