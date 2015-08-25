package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.utils.JNIconsole;

abstract public class Cmd {
		public String name;
		public String usageMsg;
		public String helpMsg;
	
		public Cmd(String n, String uMsg, String hMsg) {
			name = n;
			usageMsg = uMsg;
			helpMsg = hMsg;
		}
		
		public void printUsgae() {
				System.out.println("usage: "+usageMsg);
		}
	
		public void printHelpMsg() {
			System.out.println();
			printUsgae();
			System.out.println();
			System.out.println(helpMsg);
		}
		
		public boolean areYouSure() {
			byte []arr;
			
			while (true) {
				System.out.print("Are you sure (y/n)?");

				arr=JNIconsole.readline();
				if (arr!=null) { 
					if (arr[0]=='y' || arr[0]=='Y') 
						return true;
					if (arr[0]=='n' || arr[0]=='N') 
						return false;
				}
				else
					return false;
			}
		}
		
		// arguments[0] is the command
		abstract public boolean areParametersSane (String arguments[]);
		
		// called from local node
		abstract public int execute(String command);
		
		// called from remote node, need to be overwritten
		public String remote_execute(String command) {
			return "nothing to do";
		}
}
