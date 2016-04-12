
package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdChunkList extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NID = new Argument("nid", null, false, "Node ID");

	@Override
	public String getName() {

		return "chunklist";
	}

	@Override
	public String getDescription() {

		return "lists a range of nodes";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {

		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);

		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		ArrayList<Long> chunkRanges;

		chunkRanges = chunkService.getAllLocalChunkIDRanges(nid);

		for (int i = 0; i < chunkRanges.size(); i++)
		{
			long currRange = chunkRanges.get(i);
			if (i % 2 == 0) {
				System.out.println("from: " + Long.toHexString(currRange));
			} else {
				System.out.println("to:   " + Long.toHexString(currRange));
			}
		}

		return true;
	}

}
