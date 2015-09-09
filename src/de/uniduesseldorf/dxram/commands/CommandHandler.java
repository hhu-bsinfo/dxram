
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.CommandListener;

/**
 * Handling command request from remote nodes.
 * Must be registered during startup using Core.registerCmdListenerr(new CommandHandler());
 * @author Michael Schoettner 03.09.2015
 */
public final class CommandHandler implements CommandListener {

	/**
	 * Constructor
	 */
	public CommandHandler() {
	}

	@Override
	public String processCmd(final String p_command, final boolean p_needReply) {
		final String[] arguments = p_command.split(" ");
		final AbstractCmd c = Shell.getCommand(arguments[0]);

		if (c == null) {
			return "error: unknown command";
		} else {
			if (c.areParametersSane(arguments)) {
				return c.remoteExecute(p_command);
			}
		}
		return "internal error";
	}
}
