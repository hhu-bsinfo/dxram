package de.hhu.bsinfo.dxram.sync.tcmds;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Command to sign on to a barrier (for testing/debugging)
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class TcmdBarrierSignOn extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_BID =
			new ArgumentList.Argument("bid", null, false,
					"Id of the barrier to sign on to");
	private static final ArgumentList.Argument MS_ARG_DATA =
			new ArgumentList.Argument("data", "0L", true,
					"Custom data to pass along with the sign on call (optional)");

	@Override
	public String getName() {
		return "barriersignon";
	}

	@Override
	public String getDescription() {
		return "Sign on to an allocated barrier for synchronization (for testing/debugging)";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_BID);
		p_arguments.setArgument(MS_ARG_DATA);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer barrierId = p_arguments.getArgumentValue(MS_ARG_BID, Integer.class);
		Long data = p_arguments.getArgumentValue(MS_ARG_DATA, Long.class);

		SynchronizationService synchronizationService =
				getTerminalDelegate().getDXRAMService(SynchronizationService.class);

		Pair<short[], long[]> result = synchronizationService.barrierSignOn(barrierId, data);
		if (result == null) {
			getTerminalDelegate().println("Signing on to barrier " + BarrierID.toHexString(barrierId) + " failed.",
					TerminalColor.RED);
			return true;
		}

		String str = new String();
		for (int i = 0; i < result.first().length; i++) {
			str += "\n" + NodeID.toHexString(result.first()[i]) + ": " + ChunkID.toHexString(result.second()[i]);
		}

		getTerminalDelegate()
				.println("Synchronized to barrier " + BarrierID.toHexString(barrierId) + " custom data: " + str);

		return true;
	}
}
