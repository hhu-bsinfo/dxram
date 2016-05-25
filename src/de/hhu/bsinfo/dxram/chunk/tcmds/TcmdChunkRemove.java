
package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the chunkremove command which removes a chunk specified by CID or LID and NID
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 18.04.16
 */

public class TcmdChunkRemove extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID = new Argument("cid", null, true, "Full chunk id of the chunk to remove");
	private static final Argument MS_ARG_LID = new Argument("lid", null, true,
			"Local id of the chunk to remove. If missing node id, current node is assumed");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Node id to remove the chunk with specified local id");

	@Override
	public String getName() {
		return "chunkremove";
	}

	@Override
	public String getDescription() {
		return "Remove an existing chunk. Usable with either full chunk id or split into nid and lid.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Long cid = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);

		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		// we favor full cid
		if (cid != null) {
			// don't allow removal of index chunk
			if (ChunkID.getLocalID(cid) == 0) {
				getTerminalDelegate().println("Removal of index chunk is not allowed.", TerminalColor.RED);
				return true;
			}

			if (chunkService.remove(cid) != 1) {
				getTerminalDelegate().println("Removing chunk with ID " + ChunkID.toHexString(cid) + " failed.",
						TerminalColor.RED);
				return true;
			}
		} else {
			if (lid != null) {
				// don't allow removal of index chunk
				if (lid == 0) {
					getTerminalDelegate().println("Removal of index chunk is not allowed.", TerminalColor.RED);
					return true;
				}

				// check for remote id, otherwise we assume local
				if (nid == null) {
					getTerminalDelegate().println("error: missing nid for lid", TerminalColor.RED);
					return false;
				}

				// create cid
				cid = ChunkID.getChunkID(nid, lid);
				if (chunkService.remove(cid) != 1) {
					getTerminalDelegate().println("Removing chunk with ID " + ChunkID.toHexString(cid) + " failed.",
							TerminalColor.RED);
					return true;
				}
			} else {
				getTerminalDelegate().println("No cid or nid/lid specified.", TerminalColor.RED);
				return false;
			}
		}

		getTerminalDelegate().println("Chunk " + ChunkID.toHexString(cid) + " removed.");
		return true;
	}
}
