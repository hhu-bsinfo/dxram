
package de.hhu.bsinfo.dxram.sync.tcmds;

import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Get the status of an allocated barrier (currenlty signed on peers).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class TcmdBarrierStatus extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_BID =
			new ArgumentList.Argument("bid", null, false,
					"Id of an allocated barrier to get the status of");

	@Override
	public String getName() {
		return "barrierstatus";
	}

	@Override
	public String getDescription() {
		return "Get the current status of a barrier";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_BID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer barrierId = p_arguments.getArgumentValue(MS_ARG_BID, Integer.class);

		SynchronizationService synchronizationService =
				getTerminalDelegate().getDXRAMService(SynchronizationService.class);

		short[] status = synchronizationService.barrierGetStatus(barrierId);
		if (status == null) {
			getTerminalDelegate().println("Getting status of barrier " + BarrierID.toHexString(barrierId) + " failed.");
			return true;
		}

		String peers = new String();
		for (int i = 1; i < status.length; i++) {
			peers += NodeID.toHexString(status[i]) + ", ";
		}

		getTerminalDelegate().println("Barrier status " + BarrierID.toHexString(barrierId) + ", "
				+ status[0] + "/" + (status.length - 1) + ": " + peers);

		return true;
	}
}
