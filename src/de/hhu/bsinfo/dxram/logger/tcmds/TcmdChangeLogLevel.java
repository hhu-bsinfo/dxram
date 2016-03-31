package de.hhu.bsinfo.dxram.logger.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.dxram.logger.LoggerService;;

public class TcmdChangeLogLevel extends TerminalCommand{

	@Override
	public String getName() {
		
		return "change-log-level";
	}

	@Override
	public String getDescription() {
		
		return "changes log level via terminal";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		// TODO Auto-generated method stub	
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		LoggerService logService = getTerminalDelegate().getDXRAMService(LoggerService.class);
		
		
		
//		logService.
		
		return true;
	}

	
}
