
package de.hhu.bsinfo.dxram.sync.tcmds;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Command to change the size of an existing barrier.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class TcmdBarrierSize extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_BID =
			new ArgumentList.Argument("bid", null, false,
					"Id of the barrier to change its size");
	private static final ArgumentList.Argument MS_ARG_SIZE =
			new ArgumentList.Argument("size", null, false,
					"New size for the existing barrier");

	@Override
	public String getName() {
		return "barriersize";
	}

	@Override
	public String getDescription() {
		return "Change the size of an existing barrier, keeping its id";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_BID);
		p_arguments.setArgument(MS_ARG_SIZE);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer bid = p_arguments.getArgumentValue(MS_ARG_BID, Integer.class);
		Integer size = p_arguments.getArgumentValue(MS_ARG_SIZE, Integer.class);

		SynchronizationService synchronizationService =
				getTerminalDelegate().getDXRAMService(SynchronizationService.class);

		if (!synchronizationService.barrierChangeSize(bid, size)) {
			getTerminalDelegate()
			.println("Changing barrier " + BarrierID.toHexString(bid) + " size to " + size + " failed.",
					TerminalColor.RED);
		} else {
			getTerminalDelegate().println("Barrier " + BarrierID.toHexString(bid) + " size changed to " + size);
		}

		return true;
	}
}
