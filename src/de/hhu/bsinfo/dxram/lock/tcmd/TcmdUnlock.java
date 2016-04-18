
package de.hhu.bsinfo.dxram.lock.tcmd;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.lock.AbstractLockService.ErrorCode;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Command to unlock a previously locked chunk
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.04.16
 */
public class TcmdUnlock extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID =
			new Argument("cid", null, true, "Full chunk ID of the chunk to get data from");
	private static final Argument MS_ARG_LID =
			new Argument("lid", null, true, "Separate local id part of the chunk to get data from");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Separate node id part of the chunk to get data from");

	@Override
	public String getName() {

		return "chunkunlock";
	}

	@Override
	public String getDescription() {

		return "Unlock a previously locked chunk";
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

		ErrorCode err = lockService.unlock(true, chunkId);
		if (err != ErrorCode.SUCCESS) {
			System.out.println("Error unlocking chunk " + ChunkID.toHexString(chunkId) + ": " + err);
		} else {
			System.out.println("Unlocked chunk " + ChunkID.toHexString(chunkId));
		}

		return true;
	}
}
