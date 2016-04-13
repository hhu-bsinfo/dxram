
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Terminal command to print a string to the console
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalCommandPrint extends AbstractTerminalCommand {

	private static final Argument MS_ARG_MSG = new Argument("msg", null, false, "Message to print");

	@Override
	public String getName() {
		return "print";
	}

	@Override
	public String getDescription() {
		return "Print a message";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_MSG);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		String msg = p_arguments.getArgumentValue(MS_ARG_MSG, String.class);
		System.out.println(msg);
		return true;
	}

}
