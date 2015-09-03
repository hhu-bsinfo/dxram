package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;

public class CmdQuit extends Cmd {

	public String get_name()          {   return "quit";   }
	public String get_usage_message() {   return "quit";   }
	public String get_help_message()  {   return "Quit console and shutdown node.";   }
	public String get_syntax()        {   return "quit";   }

	
	// called after parameter have been checked
	public int execute(String p_command) {
		if (areYouSure()==false) return 1;
		Core.close();
		System.exit(0);
		return 0;
	}
}
