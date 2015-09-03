
package de.uniduesseldorf.dxram.commands;

public class CmdClear extends Cmd {

	@Override
	public String get_name() {
		return "clear";
	}

	@Override
	public String get_usage_message() {
		return "clear";
	}

	@Override
	public String get_help_message() {
		return "Clears the console.";
	}

	@Override
	public String get_syntax() {
		return "clear";
	}

	// called after parameter have been checked
	@Override
	public int execute(final String p_command) {
		// ANSI escape codes (clear screen, move cursor to first row and first column)
		System.out.print("\033[H\033[2J");
		System.out.flush();
		return 0;
	}
}
