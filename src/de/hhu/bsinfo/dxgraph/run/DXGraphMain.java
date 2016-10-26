package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxcompute.run.DXComputeMain;
import de.hhu.bsinfo.dxgraph.DXGraph;

/**
 * Base class for an entry point of a DXGraph application.
 * If DXGraph is integrated into an existing application,
 * use the DXGraph class instead.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 12.09.16
 */
public class DXGraphMain extends DXComputeMain {

	/**
	 * Default constructor
	 */
	public DXGraphMain() {
		super("DXGraph", new DXGraph());
	}

	/**
	 * Constructor
	 *
	 * Use this if you extended the DXGraph class and provide an instance of it to
	 * run it within the DXRAMMain context
	 *
	 * @param p_applicationName New application name for this DXGraph instance
	 * @param p_dxgraph DXCompute instance to run (just create the instance, no init)
	 */
	public DXGraphMain(final String p_applicationName, final DXGraph p_dxgraph) {
		super(p_applicationName, p_dxgraph);
	}

	/**
	 * Main entry point
	 *
	 * @param p_args Program arguments.
	 */
	public static void main(final String[] p_args) {
		DXGraphMain dxgraph = new DXGraphMain();
		dxgraph.run(p_args);
	}
}
