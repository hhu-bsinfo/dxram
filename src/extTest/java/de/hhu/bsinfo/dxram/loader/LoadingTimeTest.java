package de.hhu.bsinfo.dxram.loader;

import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class LoadingTimeTest {
    @TestInstance(runOnNodeIdx = 1)
    public void timingTest(final DXRAM p_instance) throws Exception {
        List<Long> testData = new ArrayList();
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("src/extTest/resources/dxrest-1.jar"));

        for (int i = 0; i < 1000; i++) {
            loaderService.cleanLoader();

            long start = System.nanoTime();
            loaderService.findClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
            long stop = System.nanoTime();

            testData.add(stop - start);
        }

        FileWriter writer = new FileWriter("loadingTime.txt");
        for (Long data : testData) {
            writer.write(data + System.lineSeparator());
        }
    }
}
