package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxgraph.DXGraph;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Base class for an entry point of a DXGraph application.
 * If DXGraph is integrated into an existing application,
 * use the DXGraph class instead.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 12.09.16
 */
public class DXGraphMain extends AbstractMain {

	private DXGraph m_dxgraph;

	/**
	 * Default constructor
	 */
	public DXGraphMain() {
		super("DXGraph main entry point.");
		m_dxgraph = new DXGraph();
	}

	/**
	 * Constructor
	 *
	 * @param p_description Override the description for main.
	 */
	public DXGraphMain(final String p_description) {
		super(p_description);
		m_dxgraph = new DXGraph();
	}

	/**
	 * Main entry point
	 *
	 * @param p_args Program arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new DXGraphMain();
		main.run(p_args);
	}

	@Override
	protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
	}

	@Override
	protected int main(final ArgumentList p_arguments) {
		if (!m_dxgraph.initialize(true)) {
			System.out.println("Initializing DXGraph failed.");
			return -1;
		}

		return mainApplication(p_arguments);
	}

	/**
	 * Override this to implement your application built on top of DXGraph.
	 *
	 * @param p_arguments Arguments provided by the application.
	 * @return Exit code of the application.
	 */
	protected int mainApplication(final ArgumentList p_arguments) {
		NodeRole role = getService(BootService.class).getNodeRole();

		if (role == NodeRole.TERMINAL) {
			System.out.println(">>> DXGraph Terminal started <<<");
			if (!runTerminal()) {
				return -1;
			} else {
				return 0;
			}
		} else {
			System.out.println(">>> DXGraph started <<<");

			while (true) {
				// Wait
				try {
					Thread.sleep(100000);
				} catch (final InterruptedException e) {
				}
			}
		}
	}

	/**
	 * Get a service from DXGraph.
	 *
	 * @param <T>     Type of the implemented service.
	 * @param p_class Class of the service to get.
	 * @return DXRAM service or null if not available.
	 */
	protected <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
		return m_dxgraph.getService(p_class);
	}

	/**
	 * Get the DXGraph instance.
	 *
	 * @return DXGraph instance.
	 */
	protected DXGraph getDXGraph() {
		return m_dxgraph;
	}

	/**
	 * Run the built in terminal. The calling thread will be used for this.
	 *
	 * @return True if execution was successful and finished, false if starting the terminal failed.
	 */
	protected boolean runTerminal() {
		TerminalService term = getService(TerminalService.class);
		if (term == null) {
			System.out.println("ERROR: Cannot run terminal, missing service.");
			return false;
		}

		term.loop();
		return true;
	}
}
