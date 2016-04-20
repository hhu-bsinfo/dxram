
package de.hhu.bsinfo.dxram.nameservice.tcmds;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Command to register a nameservice mapping.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.02.16
 */
public class TcmdNameRegister extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID =
			new Argument("cid", null, true, "Full chunk ID of the data to register");
	private static final Argument MS_ARG_LID =
			new Argument("lid", null, true, "Separate local id part of the data to register");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Separate node id part of the data to register");
	private static final Argument MS_ARG_NAME = new Argument("name", null, false, "Name to register the chunk id for");

	@Override
	public String getName() {
		return "namereg";
	}

	@Override
	public String getDescription() {
		return "Register a nameservice entry for a specific chunk id.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_NAME);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		Long cid = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		String name = p_arguments.getArgumentValue(MS_ARG_NAME, String.class);

		NameserviceService nameservice = getTerminalDelegate().getDXRAMService(NameserviceService.class);

		long chunkId = -1;
		// we favor full cid
		if (cid != null) {
			chunkId = cid;
		} else {
			if (lid != null) {
				if (nid == null) {
					System.out.println("error: missing nid for lid");
					return false;
				}

				// create cid
				chunkId = ChunkID.getChunkID(nid, lid);
			} else {
				System.out.println("No cid or nid/lid specified.");
				return false;
			}
		}

		if (name.length() > 5) {
			System.out.println("Max name length allowed: 5");
			return true;
		}

		nameservice.register(chunkId, name);

		return true;
	}

}
