package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.CommandListener;

// must be registered during startup using Core.registerCmdListenerr(new CommandHandler());
public class CommandHandler implements CommandListener {
	
	public String processCmd(String p_command,  boolean p_needReply) {
		String[] arguments = p_command.split(" ");
		Cmd c = Shell.m_commandMap.get(arguments[0]);
				
		if (c==null)
			return "error: unknown command";
		else {
			if (c.areParametersSane(arguments))
				return c.remote_execute(p_command);
		}	
		return "internal error";
	}
}
