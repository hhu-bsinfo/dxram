package de.uniduesseldorf.dxram.commands;

import java.util.Set;

public class CmdHelp extends Cmd {

	public String get_name()          {   return "help";   }
	public String get_usage_message() {   return "help [command]";   }
	public String get_help_message()  {   return "Shows help information.\nhelp: list all commands\nhelp command: show help information about given 'command'";   }
	public String get_syntax()        {   return "help [STR]";   }

	
	// called after parameter have been checked
	public int execute(String p_command) {
		String[] arguments;

		arguments = p_command.split(" ");
		
		if (arguments.length==1) { 
			Set<String> s = Shell.m_commandMap.keySet();
			System.out.println("known commands: " + s.toString());
		}
		else {
			Cmd c = Shell.m_commandMap.get(arguments[1]);
			if (c==null) {
				System.out.println("  error: unknown command '"+arguments[1]+"'");
				return -1;
			}
				
			c.printHelpMsg();
		}
		return 0;
	}
}
