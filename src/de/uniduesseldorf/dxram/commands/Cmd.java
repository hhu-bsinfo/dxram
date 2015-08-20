package de.uniduesseldorf.dxram.commands;

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
				System.out.println(usageMsg);
		}
	
		public void printHelpMsg() {
			System.out.println();
			printUsgae();
			System.out.println();
			System.out.println(helpMsg);
		}
		
		abstract public boolean areParametersSane (String arguments[]);
		
		abstract public int execute(String command);
		
}
