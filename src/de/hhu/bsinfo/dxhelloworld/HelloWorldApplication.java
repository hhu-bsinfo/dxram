package de.hhu.bsinfo.dxhelloworld;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * "Hello world" example DXRAM application
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class HelloWorldApplication extends AbstractApplication {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return DXRAM.VERSION;
    }

    @Override
    public String getApplicationName() {
        return "HelloWorld";
    }

    @Override
    public boolean useConfigurationFile() {
        return false;
    }

    @Override
    public void main() {
        BootService bootService = getService(BootService.class);

        System.out.println("Hello, I am application " + getApplicationName() + " on a peer and my node id is " + NodeID.toHexString(bootService.getNodeID()));

        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }
}
