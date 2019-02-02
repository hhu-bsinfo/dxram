package de.hhu.bsinfo.dxram.app;

import org.junit.Assert;
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
public class ApplicationServiceTest2 {
    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) {
        ApplicationService appService = p_instance.getService(ApplicationService.class);

        appService.registerApplicationClass(TestApplication2.class);
        boolean result = appService.startApplication(TestApplication2.class.getName());
        Assert.assertTrue(result);

        appService.shutdownApplication(TestApplication2.class.getName());

        while (!appService.getApplicationsRunning().isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {
                // ignore
            }
        }
    }
}
