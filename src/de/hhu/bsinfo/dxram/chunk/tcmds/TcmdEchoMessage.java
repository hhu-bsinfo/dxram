package de.hhu.bsinfo.dxram.chunk.tcmds;


import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdEchoMessage extends TerminalCommand{

	
	@Override
	public String getName() {
		return "echo";
	}

	@Override
	public String getUsageMessage() {
		return "echo message[Str]:MESSAGE";
	}

	@Override
	public String getHelpMessage() {
		return "echo Command in DxRam Shell";
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		String echo_message = p_arguments.getArgumentValue("message");
		
		if(echo_message == null)
			return false;
		
		System.out.println(echo_message);
		return true;
	}
	
}
