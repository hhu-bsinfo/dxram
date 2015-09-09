
package de.uniduesseldorf.dxram.commands;

import java.util.Set;

/**
 * Help
 * @author Michael Schoettner 03.09.2015
 */
public class CmdHelp extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdHelp() {
	}

	@Override
	public String getName() {
		return "help";
	}

	@Override
	public String getUsageMessage() {
		return "help [command]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Shows help information.\nhelp: list all commands\n";
		final String line2 = "help command: show help information about given 'command'";
		return line1+line2;
	}

	@Override
	public String getSyntax() {
		return "help [STR]";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		String[] arguments;

		arguments = p_command.split(" ");

		if (arguments.length == 1) {
			final Set<String> s = Shell.getAllCommands();
			System.out.println("known commands: " + s.toString());
		} else {
			final AbstractCmd c = Shell.getCommand(arguments[1]);
			if (c == null) {
				System.out.println("  error: unknown command '" + arguments[1] + "'");
				return false;
			}

			c.printHelpMsg();
		}
		return true;
	}
}
