package de.hhu.bsinfo.dxram.sync.tcmds;

import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Command to create a new barrier.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class TcmdBarrierAlloc extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_SIZE =
			new ArgumentList.Argument("size", null, false,
					"Size of the barrier, i.e. the number of peers that have to sign on for release.");

	@Override
	public String getName() {
		return "barrieralloc";
	}

	@Override
	public String getDescription() {
		return "Create a new barrier for synchronization of mutliple peers";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_SIZE);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer size = p_arguments.getArgumentValue(MS_ARG_SIZE, Integer.class);

		SynchronizationService synchronizationService =
				getTerminalDelegate().getDXRAMService(SynchronizationService.class);

		int barrierId = synchronizationService.barrierAllocate(size);
		if (barrierId == BarrierID.INVALID_ID) {
			getTerminalDelegate().println("Allocating barrier failed.", TerminalColor.RED);
		} else {
			getTerminalDelegate()
					.println("Allocating barrier successful, barrier id: " + BarrierID.toHexString(barrierId));
		}

		return true;
	}
}
