
package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

import java.util.ArrayList;

/**
 * This class handles the chunklist command which lists all chunks from a node
 *
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 18.04.16
 */

public class TcmdChunkList extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NID =
			new Argument("nid", null, false, "Node ID of the remote peer to get the list from");
	private static final Argument MS_ARG_MIGRATED =
			new Argument("migrated", "false", true, "List the migrated chunks as well.");

	@Override
	public String getName() {
		return "chunklist";
	}

	@Override
	public String getDescription() {
		return "Get a list of chunk id ranges from a peer holding chunks (migrated chunks optional)";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_MIGRATED);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		boolean migrated = p_arguments.getArgumentValue(MS_ARG_MIGRATED, Boolean.class);
		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		ArrayList<Long> chunkRanges = chunkService.getAllLocalChunkIDRanges(nid);

		if (chunkRanges == null) {
			getTerminalDelegate().println("Getting chunk ranges failed.", TerminalColor.RED);
			return true;
		}

		getTerminalDelegate().println(
				"Locally created chunk id ranges of " + NodeID.toHexString(nid) + "(" + chunkRanges.size() / 2 + "):");
		for (int i = 0; i < chunkRanges.size(); i++) {
			long currRange = chunkRanges.get(i);
			if (i % 2 == 0) {
				getTerminalDelegate().print("[" + ChunkID.toHexString(currRange));
			} else {
				getTerminalDelegate().println(", " + ChunkID.toHexString(currRange) + "]");
			}
		}

		if (migrated) {
			chunkRanges = chunkService.getAllMigratedChunkIDRanges(nid);

			if (chunkRanges == null) {
				getTerminalDelegate().println("Getting migrated chunk ranges failed.", TerminalColor.RED);
				return true;
			}

			getTerminalDelegate().println(
					"Migrated chunk id ranges of " + NodeID.toHexString(nid) + "(" + chunkRanges.size() / 2 + "):");

			for (int i = 0; i < chunkRanges.size(); i++) {
				long currRange = chunkRanges.get(i);
				if (i % 2 == 0) {
					getTerminalDelegate().print("[" + ChunkID.toHexString(currRange));
				} else {
					getTerminalDelegate().println(", " + ChunkID.toHexString(currRange) + "]");
				}
			}
		}

		return true;
	}

}
