
package de.hhu.bsinfo.dxram.term.tcmds;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Execute terminal commands by reading them from a file (for automated testing).
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 27.04.16
 */
public class TcmdScriptExec extends AbstractTerminalCommand {
	private static final Argument MS_ARG_FILE =
			new Argument("file", null, false, ".dsh script file to read and execute");

	/**
	 * Constructor
	 */
	public TcmdScriptExec() {
	}

	@Override
	public String getName() {
		return "dsh";
	}

	@Override
	public String getDescription() {
		return "Execute a DXRAM terminal script (dsh)";
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
					getTerminalDelegate().println(
							"Error executing command from line " + lineCounter + " of file " + file + ", aborting.",
							TerminalColor.RED);
					return true;
				}
				lineCounter++;
			}
		} catch (final IOException e) {
			getTerminalDelegate().println("Error executing commands from file " + file + ": " + e.getMessage(),
					TerminalColor.RED);
		}

		return true;
	}
}
