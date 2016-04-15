
package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the chunkstatus command which shows the Chunk Service status from a specified Node
 * @author Mike
 */

public class TcmdChunkStatus extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NID =
			new Argument("nid", null, false, "Node ID of the remote peer to get the status from");
	private static final Argument MS_ARG_SIZE_TYPE =
			new Argument("sizetype", null, true, "Specify the type of size you want to display (b, kb, mb, gb)");

	@Override
	public String getName() {
		return "chunkstatus";
	}

	@Override
	public String getDescription() {
		return "Get the status of the chunk service/memory from a remote node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_SIZE_TYPE);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		String sizeType = p_arguments.getArgumentValue(MS_ARG_SIZE_TYPE, String.class);
		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		ChunkService.Status status = chunkService.getStatus(nid);

		if (status == null) {
			System.out.println("Getting status failed.");
			return true;
		}

		long divisor = 1;
		if (sizeType != null) {
			sizeType = sizeType.toLowerCase();
			if (sizeType.equals("b")) {
				divisor = 1L;
			} else if (sizeType.equals("kb")) {
				divisor = 1024L;
			} else if (sizeType.equals("mb")) {
				divisor = 1024L * 1024L;
			} else if (sizeType.equals("gb")) {
				divisor = 1024L * 1024L * 1024L;
			} else {
				// invalid, default to byte
				sizeType = "b";
			}
		} else {
			sizeType = "b";
		}

		System.out.println("Chunk service/memory status of " + NodeID.toHexString(nid) + ":");
		if (divisor == 1) {
			System.out.println("Free memory (" + sizeType + "): " + status.getFreeMemory() / divisor);
			System.out.println("Total memory (" + sizeType + "): " + status.getTotalMemory() / divisor);
		} else {
			System.out.println("Free memory (" + sizeType + "): " + status.getFreeMemory() / (double) divisor);
			System.out.println("Total memory (" + sizeType + "): " + status.getTotalMemory() / (double) divisor);
		}

		System.out.println("Number of active chunks: " + status.getNumberOfActiveChunks());

		return true;
	}

}
