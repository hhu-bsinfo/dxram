package de.uniduesseldorf.dxram.commands;

import java.io.IOException;

public class CmdClear extends Cmd {
	public static String STR_CMD = "clear";
	public static String STR_UM  = "clear";
	public static String STR_HM  = "Clears the console.";

	public CmdClear() {
		super(STR_CMD, STR_UM, STR_HM);
	}
	
	// called by shell
	public boolean areParametersSane (String arguments[]) {
		if (arguments.length == 1) 
			return true;
		printUsgae();
		return false;
	}

	// called after parameter have been checked
	public int execute(String command) {
		// ANSI escape codes (clear screen, move cursor to first row and first column)
		System.out.print("\033[H\033[2J");
		System.out.flush();
		return 0;
	}
}
