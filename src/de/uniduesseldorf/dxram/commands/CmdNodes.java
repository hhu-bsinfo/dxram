package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

public class CmdNodes extends Cmd {
	public static String STR_CMD = "nodes";
	public static String STR_UM  = "nodes superpeers|peers|all";
	public static String STR_HM  = "List active nodes known by Zookeeper.\n   superpeers: all superpeers\n   peers: all peers\n   all: all nodes";

	private static int param=-1;
	
	public CmdNodes() {
		super(STR_CMD, STR_UM, STR_HM);
	}

	// called by shell
	public boolean areParametersSane (String arguments[]) {
		param = -1;	// reset param
		if (arguments.length == 2) {
			if (arguments[1].compareTo("superpeers")==0) param = 0;  
			if (arguments[1].compareTo("peers")==0) param = 1;
			if (arguments[1].compareTo("all")==0) param = 2;
			if (param >=0) return true;
		} 
		printUsgae();
		return false;
	}
	
	// called after parameter have been checked
	public int execute(String command) {

		try {
			switch (param) {
				case 0:
					System.out.println("superpeers:");
					System.out.println("   "+ZooKeeperHandler.getChildren("nodes/superpeers").toString());
					break;
				case 1:
					System.out.println("peers:");
					System.out.println("   "+ZooKeeperHandler.getChildren("nodes/peers").toString());
				case 2:
					System.out.println("superpeers:");
					System.out.println("   "+ZooKeeperHandler.getChildren("nodes/superpeers").toString());
					System.out.println("peers:");
					System.out.println("   "+ZooKeeperHandler.getChildren("nodes/peers").toString());
					break;
			}
		} catch (final ZooKeeperException e) {
			System.out.println("error: could not access ZooKeeper!");
			return -1;
		}
		return 0;
	}
	
}
