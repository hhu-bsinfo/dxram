package de.hhu.bsinfo.dxram.boot.tcmds;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

// get information about current node on or another remote node
public class TcmdNodeInfo extends TerminalCommand {

	private static final Argument MS_ARG_NODE_ID = new Argument("nodeid", null, true, "If specified, gets information of this node, otherwise current node");
	
	@Override
	public String getName() {
		return "nodeinfo";
	}
	@Override
	public String getDescription() {
		return "Get information about either the current node or another node in the network";
	}
	
	@Override
	public void registerArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NODE_ID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments)
	{
		Short nodeID = p_arguments.getArgumentValue(MS_ARG_NODE_ID, Short.class);
		BootComponent boot = getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		
		if (nodeID == null)
		{
			// get info from own node
			System.out.println("Node info 0x" + Integer.toHexString(boot.getNodeID()).substring(4).toUpperCase() + " (" + boot.getNodeID() + "):");
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
