package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.utils.JNIconsole;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

import java.util.Arrays;

/*
 * Base class for commands
 * 
 * Syntax must be described using a string, keywords:
 * 	STR		string, any chars
 *  PNR		positive number, e.g. LID (may be 0)
 *  ANID	any existing NID
 *  PNID	any existing peer NID
 *  SNID 	any existing superpeer NID
 *  ,		for NID,LID tuples
 *  []		optional parameters (at the end only)
 */

abstract public class Cmd {

		abstract public String get_name();
	
		abstract public String get_usage_message();
		
		abstract public String get_help_message();
	
		abstract public String get_syntax();

		// called from local node; expects areParamtersSane to be called before!
		abstract public int execute(String p_command);
		
		// called from remote node, need to be overwritten
		public String remote_execute(String command) {
			return "error: remote_execute not implemented";
		}

		public void printUsgae() {
				System.out.println("  usage: "+get_usage_message());
		}
	
		public void printHelpMsg() {
			String lines[];
			
			System.out.println("  usage:       "+get_usage_message());
			System.out.println();
			
			lines = get_help_message().split("\n");
			if (lines==null) return ;	// we should never end up here
			System.out.println("  description: "+lines[0]);
			for (int i=1; i<lines.length; i++) {
				System.out.println("               "+lines[i]);
			}
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
		
		/* 
		 * Check content of one token (used by areParametersSane)
		 */
		private boolean parse_token(String expected, String found) {
			
			if (expected.compareTo("STR")==0 ||			// string
			    expected.compareTo("[STR]")==0 
			   ) {			
				// nothing to do, anything allowed
				;
			}
			else if (expected.compareTo("PNR")==0 ||	//positive number (including 0)
					 expected.compareTo("[PNR]")==0 	
					) {	
				try {
					Long.parseLong(found);
				}
				catch(NumberFormatException nfe) {
					System.out.println("  error: expected positive number but found '"+found+"'");
					return false;
				}
			}
			else if (expected.compareTo("ANID")==0   ||			// any NID (peer or superpeer)
					 expected.compareTo("[ANID]")==0 ||
					 expected.compareTo("PNID")==0   ||   		// peer NID
					 expected.compareTo("[PNID]")==0 ||   		
					 expected.compareTo("SNID")==0   ||			// super peer NID
					 expected.compareTo("[SNID]")==0   			
					) {	
				
				// do we have a short number?
				try {
					Short.parseShort(found);
				}
				catch(NumberFormatException nfe) {
					System.out.println("  error: expected NID number but found '"+found+"'");
					return false;
				}
				
				if (CmdUtils.checkNID(found).compareTo("unknown")==0) {
					System.out.println("  error: unknwon NID '"+found+"'");
					return false;
					
				}
				
				if (expected.compareTo("PNID")==0 ||			// peer NID
					expected.compareTo("[PNID]")==0
						) {		
					if (CmdUtils.checkNID(found).compareTo("superpeer")==0) {
						System.out.println("  error: superpeer NID not allowed '"+found+"'");
						return false;
					}
				}
				else if (expected.compareTo("SNID")==0 ||		// superpeer NID
						 expected.compareTo("[SNID]")==0
						) {	
					if (CmdUtils.checkNID(found).compareTo("peer")==0) {
						System.out.println("  error: peer NID not allowed '"+found+"'");
						return false;
					}
				}
			}	
			return true;
		}

		// must be overwritten and complemented
		// (arguments[0] is the command)
		public boolean areParametersSane (String p_arguments[]) {
			String token[] = get_syntax().split(" ");
			String subtoken[];		
			
			// too many params?
			//System.out.println("token.length="+token.length+", p_arguments.length="+p_arguments.length);
			if (p_arguments.length>token.length) {
				System.out.println("  error: too many arguments");
				return false;
			}
			
			// parse and check params of command
			for (int i=1; i<token.length; i++) {
				
				// not enough params?
				if (i>=p_arguments.length) {
					if (token[i].indexOf('[')>=0) return true;	// just an optional missing 
					System.out.println("  error: argument missing");
					printUsgae();
					return false;
				}
				
				// check next expected symbol?
				//System.out.println(i+". token:"+token[i]);
				
				// is a tuple NID,LID expected next?
				if (token[i].indexOf(',')>=0) {	// parse NID,LID tuple
					subtoken = token[i].split(",");
					String subarg[] = p_arguments[i].split(",");
					if ( (subarg==null) || (subarg.length<2) ) {
						System.out.println("  error: expected NID,LID tuple but found '"+p_arguments[i]+"'");
						return false;
					}
					else {
						if (parse_token(subtoken[0], subarg[0])==false) return false;
						if (parse_token(subtoken[1], subarg[1])==false) return false;
					}
				}
				
				if (parse_token(token[i], p_arguments[i])==false) 
					return false;
			}
			
			return true;
		}
		
		
}
