package de.hhu.bsinfo.dxram.ms;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.ms.script.TaskScriptNodeResultCondition;
import de.hhu.bsinfo.dxram.ms.tasks.DummyTask;
import de.hhu.bsinfo.dxram.ms.tasks.EmptyTask;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.MASTER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE),
        })
public class MasterSlaveServiceScriptCondition1Test {
    @TestInstance(runOnNodeIdx = 1)
    public void conditionTest(final DXRAM p_instance) {
        MasterSlaveComputeService computeService = p_instance.getService(MasterSlaveComputeService.class);

        EmptyTask task = new EmptyTask();
        DummyTask task2 = new DummyTask();

        TaskScript script1 = new TaskScript(task);
        TaskScript script2 = new TaskScript(task2);

        TaskScriptNodeResultCondition cond = new TaskScriptNodeResultCondition(
                TaskScriptNodeResultCondition.CONDITION_EQUALS, 0, script1, script2);

        TaskScript script = new TaskScript(cond);

        TaskScriptState state = computeService.submitTaskScript(script);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        // wait for master to enter idle before shutting down
        while (computeService.getStatusMaster((short) 0).getState() != AbstractComputeMSBase.State.STATE_IDLE) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
    }
}
