
package de.hhu.bsinfo.dxram.boot.tcmds;

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * List all available nodes
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.04.16
 */
public class TcmdNodeList extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NODE_ROLE =
			new Argument("role", null, true, "Filter list by role if specified");

	@Override
	public String getName() {
		return "nodelist";
	}

	@Override
	public String getDescription() {
		return "List all available nodes or nodes of a specific type";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NODE_ROLE);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		String strRole = p_arguments.getArgumentValue(MS_ARG_NODE_ROLE, String.class);

		if (strRole != null) {
			NodeRole roleFilter = NodeRole.toNodeRole(strRole);
			BootService boot = getTerminalDelegate().getDXRAMService(BootService.class);
			List<Short> nodeIDs = boot.getIDsOfOnlineNodes();
			System.out.println("Filtering by role " + roleFilter);
			System.out.println("Total available nodes (" + nodeIDs.size() + "):");
			for (short nodeId : nodeIDs) {
				NodeRole curNodeRole = boot.getNodeRole(nodeId);
				if (roleFilter.equals(curNodeRole)) {
					System.out.println("\t" + NodeID.toHexString(nodeId) + ", " + curNodeRole + ", "
							+ boot.getNodeAddress(nodeId));
				}
			}
		} else {
			BootService boot = getTerminalDelegate().getDXRAMService(BootService.class);
			List<Short> nodeIDs = boot.getIDsOfOnlineNodes();
			System.out.println("Available nodes (" + nodeIDs.size() + "):");
			for (short nodeId : nodeIDs) {
				System.out.println("\t" + NodeID.toHexString(nodeId) + ", " + boot.getNodeRole(nodeId) + ", "
						+ boot.getNodeAddress(nodeId));
			}
		}

		return true;
	}

}
