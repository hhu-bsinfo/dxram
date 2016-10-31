
package de.hhu.bsinfo.dxcompute.run;

import de.hhu.bsinfo.dxcompute.DXCompute;
import de.hhu.bsinfo.dxram.run.DXRAMMain;

/**
 * Base class for an entry point of a DXCompute application.
 * If DXCompute is integrated into an existing application,
 * just use the DXCompute class instead.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class DXComputeMain extends DXRAMMain {

    /**
     * Default constructor
     */
    public DXComputeMain() {
        super("DXCompute", new DXCompute());
    }

    /**
     * Constructor
     * Use this if you extended the DXCompute class and provide an instance of it to
     * run it within the DXRAMMain context
     * @param p_applicationName
     *            New application name for this DXCompute instance
     * @param p_dxcompute
     *            DXCompute instance to run (just create the instance, no init)
     */
    public DXComputeMain(final String p_applicationName, final DXCompute p_dxcompute) {
        super(p_applicationName, p_dxcompute);
    }

    /**
     * Main entry point
     * @param p_args
     *            Program arguments.
     */
    public static void main(final String[] p_args) {
        DXComputeMain dxcompute = new DXComputeMain();
        dxcompute.run(p_args);
    }
}
