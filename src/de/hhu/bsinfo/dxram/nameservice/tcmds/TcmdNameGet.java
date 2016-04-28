
package de.hhu.bsinfo.dxram.nameservice.tcmds;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Get a registered name mapping from the nameservice
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.02.16
 */
public class TcmdNameGet extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NAME =
			new Argument("name", null, false, "Name to get the chunk id for");

	@Override
	public String getName() {
		return "nameget";
	}

	@Override
	public String getDescription() {
		return "Get the chunk id for a registered name mapping";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NAME);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		String name = p_arguments.getArgumentValue(MS_ARG_NAME, String.class);

		NameserviceService nameservice = getTerminalDelegate().getDXRAMService(NameserviceService.class);

		long chunkId = nameservice.getChunkID(name, 2000);

		if (chunkId == -1) {
			getTerminalDelegate().println("Could not get name entry for " + name + ", does not exist",
					TerminalColor.RED);
		} else {
			getTerminalDelegate().println(name + ": " + ChunkID.toHexString(chunkId));
		}

		return true;
	}

}
