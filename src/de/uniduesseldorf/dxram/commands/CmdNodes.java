
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

public class CmdNodes extends Cmd {

	@Override
	public String get_name() {
		return "nodes";
	}

	@Override
	public String get_usage_message() {
		return "nodes superpeers|peers|all";
	}

	@Override
	public String get_help_message() {
		return "List active nodes known by Zookeeper.\n   superpeers: all superpeers\n   peers: all peers\n   all: all nodes";
	}

	@Override
	public String get_syntax() {
		return "nodes STR";
	}

	private static int get_param(final String[] p_arguments) {
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

	// called by shell
	@Override
	public boolean areParametersSane(final String[] p_arguments) {
		if (!super.areParametersSane(p_arguments)) {
			return false;
		}

		if (get_param(p_arguments) >= 0) {
			return true;
		}

		printUsgae();
		return false;
	}

	// called after parameter have been checked
	@Override
	public int execute(final String p_command) {
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			switch (get_param(arguments)) {
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
			return -1;
		}
		return 0;
	}
}
