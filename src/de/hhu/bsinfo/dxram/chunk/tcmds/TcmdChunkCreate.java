
package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the chunkcreate command which creates a chunk via the terminal
 *
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 18.04.16
 */

public class TcmdChunkCreate extends AbstractTerminalCommand {

	private static final Argument MS_ARG_SIZE = new Argument("size", null, false, "Size of the chunk to create");
	private static final Argument MS_ARG_NODEID =
			new Argument("nid", null, false, "Node id of the peer to create the chunk on");

	@Override
	public String getName() {
		return "chunkcreate";
	}

	@Override
	public String getDescription() {
		return "Create a chunk on a remote node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_SIZE);
		p_arguments.setArgument(MS_ARG_NODEID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		Integer size = p_arguments.getArgumentValue(MS_ARG_SIZE, Integer.class);
		Short nodeID = p_arguments.getArgumentValue(MS_ARG_NODEID, Short.class);

		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		long[] chunkIDs = null;

		chunkIDs = chunkService.createRemote(nodeID, size);

		if (chunkIDs != null) {
			getTerminalDelegate().println("Created chunk of size " + size + ": " + ChunkID.toHexString(chunkIDs[0]));
		} else {
			getTerminalDelegate().println("Creating chunk failed.", TerminalColor.RED);
		}

		return true;
	}

}
