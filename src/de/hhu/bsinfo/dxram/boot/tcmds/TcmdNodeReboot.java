package de.hhu.bsinfo.dxram.boot.tcmds;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Reboot a dxram node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.05.16
 */
public class TcmdNodeReboot extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_NID =
			new ArgumentList.Argument("nid", null, false, "Id of the node to reboot");

	/**
	 * Constructor
	 */
	public TcmdNodeReboot() {
	}

	@Override
	public String getName() {
		return "nodereboot";
	}

	@Override
	public String getDescription() {
		return "Reboot a DXRAM node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		short nid = p_arguments.getArgument(MS_ARG_NID).getValue(Short.class);

		BootService bootService = getTerminalDelegate().getDXRAMService(BootService.class);

		if (!bootService.rebootNode(nid)) {
			getTerminalDelegate()
					.println("Rebooting node " + NodeID.toHexString(nid) + " failed.", TerminalColor.RED);
		} else {
			getTerminalDelegate().println("Rebooting node " + NodeID.toHexString(nid) + " successful.");
		}

		return true;
	}
}
