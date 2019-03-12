package de.hhu.bsinfo.dxram.migration;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.util.ChunkHelper;
import de.hhu.bsinfo.dxram.util.NetworkHelper;
import de.hhu.bsinfo.dxram.util.NodeRole;

import static org.junit.Assert.assertEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class MigrationServiceTest {

    private static final int CHUNK_SIZE = 32;

    private static final int CHUNK_COUNT = 128;

    @TestInstance(runOnNodeIdx = 2)
    public void testMigration(final DXRAM p_instance) {
        BootService bootService = p_instance.getService(BootService.class);
        MigrationService migrationService = p_instance.getService(MigrationService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);

        LongRange range = ChunkHelper.createChunks(chunkService, chunkLocalService, CHUNK_SIZE, CHUNK_COUNT);

        short target = NetworkHelper.findStorageNode(bootService);

        MigrationTicket ticket = migrationService.migrateRange(range.getFrom(), range.getTo(), target);
        MigrationStatus status = migrationService.await(ticket);

        assertEquals(MigrationStatus.OK, status);

        for (long chunkId = range.getFrom(); chunkId < range.getTo(); chunkId++) {
            ChunkByteArray byteArray = ChunkHelper.getChunk(chunkService, CHUNK_SIZE, chunkId);
            assertEquals(ChunkState.OK, byteArray.getState());
        }
    }
}
