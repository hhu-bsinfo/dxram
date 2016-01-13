
package de.hhu.bsinfo.dxram.commands;

import java.util.Set;

/**
 * Help
 * @author Michael Schoettner 15.09.2015
 */
public class CmdHelp extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdHelp() {}

	@Override
	public String getName() {
		return "help";
	}

	@Override
	public String getUsageMessage() {
		return "help command|all";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Shows help information.\nhelp: list all commands\n";
		final String line2 = "help command: show help information about given 'command'";
		return line1 + line2;
	}

	@Override
	public String[] getMandParams() {
        final String[] ret = {"STR"};
        return ret;
	}

	@Override
    public  String[] getOptParams() {
        return null;
    }

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		String[] arguments;

		arguments = p_command.split(" ");

		if (arguments.length == 2 && arguments[1].compareTo("all")==0) {
			final Set<String> s = Shell.getAllCommands();
			System.out.println("  All known commands: " + s.toString());
		} else {
			final AbstractCmd c = Shell.getCommand(arguments[1]);
			if (c == null) {
				System.out.println("  error: unknown command '" + arguments[1] + "'");
				ret = false;
			} else {
				c.printHelpMsg();
			}
		}

		return ret;
	}
}
