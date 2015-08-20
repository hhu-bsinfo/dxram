package de.uniduesseldorf.dxram.commands;

import java.io.IOException;
import java.util.Set;

public class CmdHelp extends Cmd {
	public static String STR_CMD = "help";
	public static String STR_UM  = "help [command]";
	public static String STR_HM  = "Shows help information.\n   help: list all commands\n   help command: show help information about given command";

	public CmdHelp() {
		super(STR_CMD, STR_UM, STR_HM);
	}
	
	// called by shell
	public boolean areParametersSane (String arguments[]) {
		if (arguments.length==1 || arguments.length==2) 
			return true;
		printUsgae();
		return false;
	}

	// called after parameter have been checked
	public int execute(String command) {
		String[] arguments;

		arguments = command.split(" ");
		
		if (arguments.length==1) { 
			Set<String> s = Shell.m_commandMap.keySet();
			System.out.println("known commands: " + s.toString());
		}
		else {
			Cmd c = Shell.m_commandMap.get(arguments[1]);
			c.printHelpMsg();
		}
		return 0;
	}
}
