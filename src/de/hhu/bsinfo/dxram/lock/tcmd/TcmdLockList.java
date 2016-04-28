
package de.hhu.bsinfo.dxram.lock.tcmd;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Command to list all locked chunks of a node
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.04.16
 */
public class TcmdLockList extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Get the list of locked chunks from a remote node");

	@Override
	public String getName() {

		return "chunklocklist";
	}

	@Override
	public String getDescription() {

		return "Get the list of all locked chunks of a node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);

		AbstractLockService lockService = getTerminalDelegate().getDXRAMService(AbstractLockService.class);
		BootService bootService = getTerminalDelegate().getDXRAMService(BootService.class);

		if (nid == null) {
			nid = bootService.getNodeID();
		}

		ArrayList<Pair<Long, Short>> list = lockService.getLockedList(nid);

		if (list == null) {
			getTerminalDelegate().println("Getting list of locked chunks failed.", TerminalColor.RED);
			return true;
		}

		getTerminalDelegate().println("Locked chunks of " + NodeID.toHexString(nid) + "(" + list.size() + ":");
		for (Pair<Long, Short> entry : list) {
			getTerminalDelegate().println(entry.first() + ": " + entry.second());
		}

		return true;
	}
}
