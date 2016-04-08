package de.hhu.bsinfo.dxram.stats.tcmds;


import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdAvailableStatisticsRecorders extends AbstractTerminalCommand{

	@Override
	public String getName() {
	
		return "available-recorders";
	}

	@Override
	public String getDescription() {
		
		return "prints available statistics recorders";
		
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		
		
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		
		StatisticsService statService = getTerminalDelegate().getDXRAMService(StatisticsService.class);
		
		System.out.println();
		
		for(StatisticsRecorder rec : statService.getRecorders())
		{
			System.out.println("\t" + rec.getName());
		}
		
		return true;
	}

}
