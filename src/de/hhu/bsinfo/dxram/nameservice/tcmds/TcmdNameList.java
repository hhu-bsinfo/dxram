
package de.hhu.bsinfo.dxram.nameservice.tcmds;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * List all available name service mappings of the system
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.02.16
 */
public class TcmdNameList extends AbstractTerminalCommand {

	@Override
	public String getName() {
		return "namelist";
	}

	@Override
	public String getDescription() {
		return "List all registered name mappings of the nameservice";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		NameserviceService nameservice = getTerminalDelegate().getDXRAMService(NameserviceService.class);

		ArrayList<Pair<String, Long>> entries = nameservice.getAllEntries();
		getTerminalDelegate().println("Nameservice entries(" + entries.size() + "):");
		for (Pair<String, Long> entry : entries) {
			getTerminalDelegate().println(entry.first() + ": " + ChunkID.toHexString(entry.second()));
		}

		return true;
	}

}
