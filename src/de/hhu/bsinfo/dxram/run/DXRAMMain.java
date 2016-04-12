
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.02.16
 */
public class DXRAMMain extends AbstractMain {

	private static final Argument ARG_DXRAM_TERMINAL =
			new Argument("dxramTerminal", "false", true, "Run the built in terminal of dxram after system launch");

	private DXRAM m_dxram;

	/**
	 * Default constructor
	 */
	public DXRAMMain() {
		super("DXRAM main entry point.");
		m_dxram = new DXRAM();
	}

	/**
	 * Constructor
	 * @param p_description
	 *            Override the description for main.
	 */
	public DXRAMMain(final String p_description) {
		super(p_description);
		m_dxram = new DXRAM();
	}

	/**
	 * Main entry point
	 * @param p_args
	 *            Program arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new DXRAMMain();
		main.run(p_args);
	}

	@Override
	protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_DXRAM_TERMINAL);
	}

	@Override
	protected int main(final ArgumentList p_arguments) {
		if (!m_dxram.initialize(true)) {
			System.out.println("Initializing DXRAM failed.");
			return -1;
		}

		return mainApplication(p_arguments);
	}

	/**
	 * Override this to implement your application built on top of DXRAM.
	 * @param p_arguments
	 *            Arguments provided by the application.
	 * @return Exit code of the application.
	 */
	protected int mainApplication(final ArgumentList p_arguments) {
		boolean runTerminal = p_arguments.getArgument(ARG_DXRAM_TERMINAL).getValue(Boolean.class);

		if (runTerminal) {
			System.out.println(">>> DXRAM Terminal started <<<");
			if (!runTerminal()) {
				return -1;
			} else {
				return 0;
			}
		} else {
			System.out.println(">>> DXRAM started <<<");

			while (true) {
				// Wait a moment
				try {
					Thread.sleep(3000);
				} catch (final InterruptedException e) {}
			}
		}
	}

	/**
	 * Get a service from DXRAM.
	 * @param <T>
	 *            Type of the implemented service.
	 * @param p_class
	 *            Class of the service to get.
	 * @return DXRAM service or null if not available.
	 */
	protected <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
		return m_dxram.getService(p_class);
	}

	/**
	 * Get the DXRAM instance.
	 * @return DXRAM instance.
	 */
	protected DXRAM getDXRAM() {
		return m_dxram;
	}

	/**
	 * Run the built in terminal. The calling thread will be used for this.
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
