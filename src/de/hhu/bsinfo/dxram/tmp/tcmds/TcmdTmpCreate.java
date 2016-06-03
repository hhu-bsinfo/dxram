
package de.hhu.bsinfo.dxram.tmp.tcmds;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Create a new chunk in temporary (superpeer storage) memory.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TcmdTmpCreate extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_SIZE = new ArgumentList.Argument("size", null, false, "Size of the chunk to create");
	private static final ArgumentList.Argument MS_ARG_ID =
			new ArgumentList.Argument("id", null, false, "Id to identify the chunk in the storage");

	@Override
	public String getName() {
		return "tmpcreate";
	}

	@Override
	public String getDescription() {
		return "Allocate memory for a chunk on a superpeer's storage (temporary)";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_SIZE);
		p_arguments.setArgument(MS_ARG_ID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		Integer size = p_arguments.getArgumentValue(MS_ARG_SIZE, Integer.class);
		Integer id = p_arguments.getArgumentValue(MS_ARG_ID, Integer.class);

		TemporaryStorageService tmpStorageService =
				getTerminalDelegate().getDXRAMService(TemporaryStorageService.class);
		if (tmpStorageService.create(id, size)) {
			getTerminalDelegate().println("Created chunk of size " + size + " in temporary storage: "
					+ ChunkID.toHexString(id));
		} else {
			getTerminalDelegate().println("Creating chunk in temporary storage failed.", TerminalColor.RED);
		}

		return true;
	}
}
