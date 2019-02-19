package de.hhu.bsinfo.dxram.ms;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER, networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.MASTER,
                        networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE,
                        networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE,
                        networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE,
                        networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE,
                        networkRequestResponseTimeoutMs = 5000),
        })
public class MasterSlaveServiceChunkInc4Test {
    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) {
        BootService boot = p_instance.getService(BootService.class);
        MasterSlaveComputeService computeService = p_instance.getService(MasterSlaveComputeService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        TestIncChunk chunk = new TestIncChunk();
        Assert.assertEquals(1, chunkService.create().create(boot.getNodeID(), chunk));
        Assert.assertEquals(ChunkState.OK, chunk.getState());
        Assert.assertNotEquals(ChunkID.INVALID_ID, chunk.getID());

        Assert.assertTrue(chunkService.put().put(chunk));

        TestIncChunkTask task = new TestIncChunkTask(chunk.getID());
        TaskScript script = new TaskScript((short) 4, (short) 4, task);

        TaskScriptState state = computeService.submitTaskScript(script);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        chunkService.get().get(chunk);
        Assert.assertEquals(4, chunk.getCounter());

        // wait for master to enter idle before shutting down
        while (computeService.getStatusMaster((short) 0).getState() != AbstractComputeMSBase.State.STATE_IDLE) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
    }
}
