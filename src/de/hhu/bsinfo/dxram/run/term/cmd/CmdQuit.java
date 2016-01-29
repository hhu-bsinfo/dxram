
package de.hhu.bsinfo.dxram.run.term.cmd;

/**
 * Quit monitor.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdQuit extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdQuit() {}

	@Override
	public String getName() {
		return "quit";
	}

	@Override
	public String getUsageMessage() {
		return "quit";
	}

	@Override
	public String getHelpMessage() {
		return "Quit console and shutdown node.";
	}

	@Override
	public String[] getMandParams() {
	    return null;
	}

	@Override
    public  String[] getOptParams() {
        return null;
    }

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;

		if (!areYouSure()) {
			ret = false;
		} else {
			System.exit(0);
		}

		return ret;
	}

}
