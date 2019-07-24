package de.hhu.bsinfo.dxram.loader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class LoadingTimeTest {
    @TestInstance(runOnNodeIdx = 1)
    public void timingTest(final DXRAM p_instance) throws Exception {
        long[] data = new long[1000];
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("src/extTest/resources/dxrest-1.jar"));

        for (int i = 0; i < 1000; i++) {
            loaderService.cleanLoader();

            long start = System.nanoTime();
            loaderService.findClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
            long stop = System.nanoTime();

            data[i] = stop - start;
        }

        BootService bootService = p_instance.getService(BootService.class);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd_HHmmss");
        BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("loadingTimes-%s-%s.txt",
                format.format(new Date()), NodeID.toHexString(bootService.getNodeID()))));
        for (long d : data) {
            writer.write(d + System.lineSeparator());
        }
        writer.close();
    }
}
