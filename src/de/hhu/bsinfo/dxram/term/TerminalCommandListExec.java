
package de.hhu.bsinfo.dxram.term;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Execute terminal commands by reading them from a file (for automated testing).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 27.04.16
 */
public class TerminalCommandListExec extends AbstractTerminalCommand {
	private static final Argument MS_ARG_FILE =
			new Argument("file", null, false, "File to read the list of commands from. Separated by new lines only");

	/**
	 * Constructor
	 */
	public TerminalCommandListExec() {}

	@Override
	public String getName() {
		return "cmdlistexec";
	}

	@Override
	public String getDescription() {
		return "Execute a list of commands read from file";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_FILE);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		String file = p_arguments.getArgument(MS_ARG_FILE).getValue(String.class);

		int lineCounter = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!getTerminalDelegate().executeTerminalCommand(line)) {
					System.out.println(
							"Error executing command from line " + lineCounter + " of file " + file + ", aborting.");
					return true;
				}
			}
		} catch (final IOException e) {
			System.out.println("Error executing commands from file " + file + ": " + e.getMessage());
		}

		return true;
	}
}
