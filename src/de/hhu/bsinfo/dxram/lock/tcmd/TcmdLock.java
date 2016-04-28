
package de.hhu.bsinfo.dxram.lock.tcmd;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.lock.AbstractLockService.ErrorCode;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Command to lock a chunk.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class TcmdLock extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID =
			new Argument("cid", null, true, "Full chunk ID of the chunk to get data from");
	private static final Argument MS_ARG_LID =
			new Argument("lid", null, true, "Separate local id part of the chunk to get data from");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Separate node id part of the chunk to get data from");

	@Override
	public String getName() {

		return "chunklock";
	}

	@Override
	public String getDescription() {

		return "Lock a chunk";
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

		AbstractLockService lockService = getTerminalDelegate().getDXRAMService(AbstractLockService.class);

		long chunkId = -1;
		// we favor full cid
		if (cid != null) {
			chunkId = cid;
		} else {
			if (lid != null) {
				if (nid == null) {
					getTerminalDelegate().println("error: missing nid for lid", TerminalColor.RED);
					return false;
				}

				// create cid
				chunkId = ChunkID.getChunkID(nid, lid);
			} else {
				getTerminalDelegate().println("No cid or nid/lid specified.", TerminalColor.RED);
				return false;
			}
		}

		ErrorCode err = lockService.lock(true, 1000, chunkId);
		if (err != ErrorCode.SUCCESS) {
			getTerminalDelegate().println("Error locking chunk " + ChunkID.toHexString(chunkId) + ": " + err,
					TerminalColor.RED);
		} else {
			getTerminalDelegate().println("Locked chunk " + ChunkID.toHexString(chunkId));
		}

		return true;
	}
}
