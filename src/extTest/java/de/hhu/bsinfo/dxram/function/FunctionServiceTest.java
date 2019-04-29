package de.hhu.bsinfo.dxram.function;

import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.function.util.ParameterList;
import de.hhu.bsinfo.dxram.util.NetworkHelper;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.data.DistributableHashMap;
import de.hhu.bsinfo.dxutils.data.holder.DistributableInteger;
import de.hhu.bsinfo.dxutils.data.holder.DistributableString;
import de.hhu.bsinfo.dxutils.serialization.ClassUtil;

import static org.junit.Assert.assertEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class FunctionServiceTest {

    private static final String INPUT_TEXT = "a, A, a, B B b B. C c c c, C C C C? D, d D d D d, C C a b B A!";

    @TestInstance(runOnNodeIdx = 2)
    public void testWordCount(final DXRAM p_instance) {

        FunctionService functionService = p_instance.getService(FunctionService.class);
        BootService bootService = p_instance.getService(BootService.class);

        short peer = NetworkHelper.findPeer(bootService);

        Assert.assertNotEquals(NodeID.INVALID_ID, peer);

        DistributableFunction function = new WordCountFunction();
        FunctionService.Status status = functionService.register(peer, WordCountFunction.NAME, function);
        assertEquals(FunctionService.Status.REGISTERED, status);

        DistributableString input = new DistributableString(INPUT_TEXT);
        DistributableHashMap<String, Long> output = functionService.executeSync(peer, WordCountFunction.NAME, input);

        assertEquals(3L, (long) output.get("a"));
        assertEquals(2L, (long) output.get("A"));
        assertEquals(2L, (long) output.get("b"));
        assertEquals(4L, (long) output.get("B"));
        assertEquals(3L, (long) output.get("c"));
        assertEquals(7L, (long) output.get("C"));
        assertEquals(3L, (long) output.get("d"));
        assertEquals(3L, (long) output.get("D"));
    }
}
