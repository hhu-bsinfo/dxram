
package de.hhu.bsinfo.dxramtodo.commands;

import de.hhu.bsinfo.utils.ZooKeeperHandler;
import de.hhu.bsinfo.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Info about nodes
 * @author Michael Schoettner 03.09.2015
 */
public class CmdNodes extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdNodes() {}

	@Override
	public String getName() {
		return "nodes";
	}

	@Override
	public String getUsageMessage() {
		return "nodes superpeers|peers|all";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "List active nodes known by Zookeeper.\n";
		final String line2 = "   superpeers: all superpeers\n   peers: all peers\n   all: all nodes";
		return line1 + line2;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"STR"};
	    return ret;
	}

	@Override
    public  String[] getOptParams() {
        return null;
    }

	/**
	 * returns a number for the argument
	 * @param p_arguments
	 *            argument
	 * @return 0,1,2
	 */
	private static int getParam(final String[] p_arguments) {
		int param = -1;

		if (p_arguments.length == 2) {
			if (p_arguments[1].compareTo("superpeers") == 0) {
				param = 0;
			}
			if (p_arguments[1].compareTo("peers") == 0) {
				param = 1;
			}
			if (p_arguments[1].compareTo("all") == 0) {
				param = 2;
			}
		}
		return param;
	}

	@Override
	public boolean areParametersSane(final String[] p_arguments) {
		boolean ret=false;

		if (!super.areParametersSane(p_arguments)) {
			ret = false;
		} else if (getParam(p_arguments) >= 0) {
			ret = true;
		}

		return ret;
	}

	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			switch (getParam(arguments)) {
			case 0:
				System.out.println("superpeers:");
				System.out.println("   " + ZooKeeperHandler.getChildren("nodes/superpeers").toString());
				break;
			case 1:
				System.out.println("peers:");
				System.out.println("   " + ZooKeeperHandler.getChildren("nodes/peers").toString());
				break;
			case 2:
				System.out.println("superpeers:");
				System.out.println("   " + ZooKeeperHandler.getChildren("nodes/superpeers").toString());
				System.out.println("peers:");
				System.out.println("   " + ZooKeeperHandler.getChildren("nodes/peers").toString());
				break;
			default:
				break;
			}
		} catch (final ZooKeeperException e) {
			System.out.println("  error: could not access ZooKeeper!");
			ret = false;
		}

		return ret;
	}
}
