
package de.hhu.bsinfo.dxram.stats.tcmds;

import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the statistic printing from the terminal command 'statsprint'
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 15.05.16
 */

public class TcmdPrintAllStatistics extends AbstractTerminalCommand {

	private static final Argument MS_ARG_STAT = new Argument("class", null, true, "filter statistics by class name");

	@Override
	public String getName() {
		return "statsprint";
	}

	@Override
	public String getDescription() {

		return "prints statistics";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_STAT);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		String filter = p_arguments.getArgumentValue(MS_ARG_STAT, String.class);

		StatisticsService statService = getTerminalDelegate().getDXRAMService(StatisticsService.class);

		if (filter == null) {
			statService.printStatistics();
		} else {
			Class<?> clss = createClassFromName(filter);

			if (clss == null) {
				return false;
			} else {
				statService.printStatistics(clss);
			}
		}

		return true;
	}

	/**
	 * Creates a class by the specified name. The whole package name is needed, the prefix 'de.hhu.bsinfo.' however is
	 * optional.
	 * @param p_className
	 *            class name of the class which will be constructed.
	 * @return
	 *         constructed class by name
	 */

	Class<?> createClassFromName(final String p_className) {

		Class<?> clss = null;
		try {
			clss = Class.forName(p_className);
		} catch (final ClassNotFoundException e) {
			// check again with longest common prefix of package names
			try {
				clss = Class.forName("de.hhu.bsinfo." + p_className);
			} catch (final ClassNotFoundException e1) {
				getTerminalDelegate().println(
						p_className + "Class not found. Did you forget to enter the package name?", TerminalColor.RED);
			}
		}

		return clss;
	}

}
