package de.hhu.bsinfo.dxram.tmp.tcmds;

import de.hhu.bsinfo.dxram.lookup.overlay.SuperpeerStorage;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Get the current status of the temporary (superpeer) storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TcmdTmpStatus extends AbstractTerminalCommand {

	@Override
	public String getName() {
		return "tmpstatus";
	}

	@Override
	public String getDescription() {
		return "Get the status of the temporary (superpeer) storage.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		TemporaryStorageService tmpStorageService =
				getTerminalDelegate().getDXRAMService(TemporaryStorageService.class);
		SuperpeerStorage.Status status = tmpStorageService.getStatus();
		if (status != null) {
			getTerminalDelegate().println("Total size occupied (bytes): " + status.calculateTotalDataUsageBytes());
			getTerminalDelegate().println(status);
		} else {
			getTerminalDelegate().println("Getting status of temporary storage failed.", TerminalColor.RED);
		}

		return true;
	}
}
