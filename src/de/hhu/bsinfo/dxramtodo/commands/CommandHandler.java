
package de.hhu.bsinfo.dxramtodo.commands;

import de.uniduesseldorf.dxram.core.dxram.CommandListener;

/**
 * Handling command request from remote nodes.
 * Must be registered during startup using Core.registerCmdListenerr(new CommandHandler());
 * @author Michael Schoettner 03.09.2015
 */
public final class CommandHandler implements CommandListener {

	/**
	 * Constructor
	 */
	public CommandHandler() {}

	@Override
	public String processCmd(final String p_command, final boolean p_needReply) {
		String ret;
		final String[] arguments = p_command.split(" ");
		final AbstractCmd c = Shell.getCommand(arguments[0]);

		if (c == null) {
			ret = "error: unknown command";
		} else {
			if (c.areParametersSane(arguments)) {
				ret = c.remoteExecute(p_command);
			} else {
				ret = "internal error";
			}
		}

		return ret;
	}
}
