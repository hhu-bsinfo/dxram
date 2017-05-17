package de.hhu.bsinfo.dxhelloworld;

import com.google.gson.annotations.Expose;

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
public class HelloWorldWithConfigApplication extends AbstractApplication {
    @Expose
    private int m_val = 5;
    @Expose
    private String m_str = "test";

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return DXRAM.VERSION;
    }

    @Override
    public String getApplicationName() {
        return "HelloWorldWithConfig";
    }

    @Override
    public boolean useConfigurationFile() {
        return true;
    }

    @Override
    public void main() {
        BootService bootService = getService(BootService.class);

        System.out.println("Hello, I am application " + getApplicationName() + " on a peer and my node id is " + NodeID.toHexString(bootService.getNodeID()));
        System.out.println("Configuration value m_val: " + m_val);
        System.out.println("Configuration value m_str: " + m_str);

        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up
    }
}
