package de.hhu.bsinfo.dxram.loader;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class LoaderSyncTest {
    @TestInstance(runOnNodeIdx = 2)
    public void register(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        BootService bootService = p_instance.getService(BootService.class);
        List<Short> superpeers = bootService.getOnlineSuperpeerNodeIDs();

        // add dxrest to cluster, wait and assert all superpeers loaded same classes
        loaderService.addJar(Paths.get("src/extTest/resources/dxrest-1.jar"));
        TimeUnit.MILLISECONDS.sleep(500);

        HashSet<Integer> counts = new HashSet<>();
        for (short nid : superpeers) {
            counts.add(loaderService.getLoadedCount(nid));
        }
        Assert.assertEquals(1, counts.size());

        // flush first superpeer, wait and assert loaded count is not the same on all superpeers
        loaderService.flushSuperpeerTable(superpeers.get(0));
        TimeUnit.MILLISECONDS.sleep(500);

        counts = new HashSet<>();
        for (short nid : superpeers) {
            counts.add(loaderService.getLoadedCount(nid));
        }
        Assert.assertTrue(counts.size() > 1);

        // force sync by loading non existent class and assert superpeers loaded same classes
        try {
            loaderService.findClass("IMPOSSIBLE_TO_LOAD");
            Assert.fail("This class should not exist");
        }catch (ClassNotFoundException e) {
            System.out.println("This is fine.");
        }
        TimeUnit.MILLISECONDS.sleep(500);

        counts = new HashSet<>();
        for (short nid : superpeers) {
            counts.add(loaderService.getLoadedCount(nid));
        }
        Assert.assertEquals(1, counts.size());

    }
}
