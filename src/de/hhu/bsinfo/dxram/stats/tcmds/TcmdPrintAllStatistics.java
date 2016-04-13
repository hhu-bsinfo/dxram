package de.hhu.bsinfo.dxram.stats.tcmds;

import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;



import de.hhu.bsinfo.dxram.stats.StatisticsService;

public class TcmdPrintAllStatistics extends TerminalCommand{

	private static final Argument MS_ARG_STAT = new Argument("class", null, true, "filter statistics by class name");
	
	@Override
	public String getName() {
		return "printstats";
	}

	@Override
	public String getDescription() {
		
		return "prints statistics";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_STAT);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		String filter =  p_arguments.getArgumentValue(MS_ARG_STAT, String.class);
		
		StatisticsService statService = getTerminalDelegate().getDXRAMService(StatisticsService.class);
		
		
		if(filter == null)
			statService.printStatistics();
		else
		{
			Class<?> clss = __createClassFromName(filter, statService);

			if(clss == null)
				return false;
			else
				statService.printStatistics(clss);
		}	
		
		return true;
	}
	
Class<?> __createClassFromName(String filter, StatisticsService statserv){
		
		Class<?> clss = null;
		try {
			clss = Class.forName(filter);
		} catch (ClassNotFoundException e) {
			// check again with longets common prefix of package names
			try {
				clss = Class.forName("de.hhu.bsinfo."+filter);
			} catch (ClassNotFoundException e1) {
				System.out.println(filter + "Class not found. Did you forget to enter the package name?");
			}
		}
		
		return clss;
	}
	
	
	
	

}
