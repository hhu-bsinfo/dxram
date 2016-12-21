package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Minimal "Hello World" example to run as DXRAM peer. This example shows how to get a service
 * and print information about the current node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 08.12.2016
 */
public final class HelloWorld extends DXRAMMain {

    /**
     * Constructor
     */
    private HelloWorld() {
        super("HelloWorld");
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        DXRAMMain main = new HelloWorld();
        main.run(p_args);
    }

    @Override
    protected int mainApplication(final String[] p_args) {
        BootService bootService = getService(BootService.class);

        System.out.println("Hello, I am a " + bootService.getNodeRole() + " and my node id is " + NodeID.toHexString(bootService.getNodeID()));

        // Put your application code running on the DXRAM node/peer here

        return 0;
    }
}
