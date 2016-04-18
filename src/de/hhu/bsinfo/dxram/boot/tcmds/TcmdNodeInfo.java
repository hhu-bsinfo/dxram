
package de.hhu.bsinfo.dxram.boot.tcmds;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Get information about the current node or another remote node.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.04.16
 */
public class TcmdNodeInfo extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NODE_ID =
			new Argument("nodeid", null, true, "If specified, gets information of this node, otherwise current node");

	@Override
	public String getName() {
		return "nodeinfo";
	}

	@Override
	public String getDescription() {
		return "Get information about either the current node or another node in the network";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NODE_ID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short nodeID = p_arguments.getArgumentValue(MS_ARG_NODE_ID, Short.class);
		BootService boot = getTerminalDelegate().getDXRAMService(BootService.class);

		System.out.println("Node info " + NodeID.toHexString(boot.getNodeID()) + ":");
		if (nodeID == null) {
			// get info from own node
			System.out.println("\tRole: " + boot.getNodeRole());
			System.out.println("\tAddress: " + boot.getNodeAddress(boot.getNodeID()));
		} else {
			// get other node
			if (boot.nodeAvailable(nodeID)) {
				System.out.println("\tRole: " + boot.getNodeRole(nodeID));
				System.out.println("\tAddress: " + boot.getNodeAddress(nodeID));
			} else {
				System.out.println("Not available.");
			}
		}

		return true;
	}

}
