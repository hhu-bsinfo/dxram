
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;

public class CmdQuit extends Cmd {

	@Override
	public String get_name() {
		return "quit";
	}

	@Override
	public String get_usage_message() {
		return "quit";
	}

	@Override
	public String get_help_message() {
		return "Quit console and shutdown node.";
	}

	@Override
	public String get_syntax() {
		return "quit";
	}

	// called after parameter have been checked
	@Override
	public int execute(final String p_command) {
		if (!areYouSure()) {
			return 1;
		}
		Core.close();
		System.exit(0);
		return 0;
	}
}
