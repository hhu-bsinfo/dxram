package de.hhu.bsinfo.dxram.tmp.tcmds;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Remove a chunk from the temporary storage (superpeer storage) memory.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TcmdTmpRemove extends AbstractTerminalCommand {
	private static final ArgumentList.Argument
			MS_ARG_ID = new ArgumentList.Argument("id", null, false, "Id of the chunk in temporary storage");

	@Override
	public String getName() {
		return "tmpremove";
	}

	@Override
	public String getDescription() {
		return "Remove a (stored) chunk from temporary storage (superpeer storage)";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_ID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer id = p_arguments.getArgumentValue(MS_ARG_ID, Integer.class);

		TemporaryStorageService tmpStorageService =
				getTerminalDelegate().getDXRAMService(TemporaryStorageService.class);

		if (tmpStorageService.remove(id)) {
			getTerminalDelegate()
					.println("Removed chunk with id " + ChunkID.toHexString(id) + " from temporary storage.");
		} else {
			getTerminalDelegate()
					.println("Removing chunk with id " + ChunkID.toHexString(id) + " failed.", TerminalColor.RED);
		}

		return true;
	}
}
