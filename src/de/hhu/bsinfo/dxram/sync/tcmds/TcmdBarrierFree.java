
package de.hhu.bsinfo.dxram.sync.tcmds;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Command to free an allocated barrier
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class TcmdBarrierFree extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_BID =
			new ArgumentList.Argument("bid", null, false,
					"Id of an allocated barrier to free");

	@Override
	public String getName() {
		return "barrierfree";
	}

	@Override
	public String getDescription() {
		return "Free an allocated barrier";
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

		if (!synchronizationService.barrierFree(barrierId)) {
			getTerminalDelegate().println("Freeing barrier failed.", TerminalColor.RED);
		} else {
			getTerminalDelegate()
			.println("Barrier " + BarrierID.toHexString(barrierId) + " free'd");
		}

		return true;
	}
}
