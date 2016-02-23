package de.hhu.bsinfo.dxram.boot.tcmds;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

// get information about current node on or another remote node
public class TcmdNodeInfo extends TerminalCommand {

	private static final String MS_ARG_NODE_ID = "nodeID";
	
	@Override
	public String getName() {
		return "nodeinfo";
	}

	@Override
	public String getUsageMessage() {
		return "nodeinfo [nodeID[short]:NID]";
	}

	@Override
	public String getHelpMessage() {
		return "Get information about either the current node or another node in the network.\n"
				+ "nodeID: other node to get information of.";
	}

	@Override
	public boolean execute(final ArgumentList p_arguments)
	{
		Short nodeID = p_arguments.getArgumentValue(MS_ARG_NODE_ID);
		BootComponent boot = getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		
		if (nodeID == null)
		{
			// get info from own node
			System.out.println("Node info (" + boot.getNodeID() + "):");
			System.out.println("\tRole: " + boot.getNodeRole());
			System.out.println("\tAddress: " + boot.getNodeAddress(boot.getNodeID()));
		}
		else
		{
			// get other node
			System.out.println("Node info (" + nodeID + "):");
			if (boot.nodeAvailable(nodeID))
			{
				System.out.println("\tRole: " + boot.getNodeRole(nodeID));
				System.out.println("\tAddress: " + boot.getNodeAddress(nodeID));
			}
			else
			{
				System.out.println("Not available.");
			}
		}
	
		return true;
	}

}
