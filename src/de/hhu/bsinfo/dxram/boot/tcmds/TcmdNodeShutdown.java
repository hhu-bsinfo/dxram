package de.hhu.bsinfo.dxram.boot.tcmds;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Shut down a dxram node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.05.16
 */
public class TcmdNodeShutdown extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_NID =
			new ArgumentList.Argument("nid", null, false, "Id of the node to shut down");
	private static final ArgumentList.Argument MS_ARG_HARD =
			new ArgumentList.Argument("hard", "true", true,
					"Specify a hard shutdown (whole application running DXRAM) or soft shutdown (DXRAM instance only)");

	/**
	 * Constructor
	 */
	public TcmdNodeShutdown() {
	}

	@Override
	public String getName() {
		return "nodeshutdown";
	}

	@Override
	public String getDescription() {
		return "Shutdown a DXRAM node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_HARD);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		short nid = p_arguments.getArgument(MS_ARG_NID).getValue(Short.class);
		boolean hard = p_arguments.getArgument(MS_ARG_HARD).getValue(Boolean.class);

		BootService bootService = getTerminalDelegate().getDXRAMService(BootService.class);

		if (!bootService.shutdownNode(nid, hard)) {
			getTerminalDelegate()
					.println("Shutting down node " + NodeID.toHexString(nid) + " failed.", TerminalColor.RED);
		} else {
			getTerminalDelegate().println("Shutting down node " + NodeID.toHexString(nid) + " successful.");
		}

		return true;
	}
}
