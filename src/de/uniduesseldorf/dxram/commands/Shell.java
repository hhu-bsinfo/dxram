package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

import java.util.HashMap;
import java.util.Map;

import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
//import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.utils.JNIconsole;

/**
 * Interactive shell
 * @author Michael Schoettner 17.08.2015
 */
public class Shell {
	// Attributes
	public static Map<String, Cmd> m_commandMap;
	
	// create & store all commands in HashMap
	static {
		Cmd c;
		
		m_commandMap = new HashMap<String, Cmd>();
		
		c = new CmdHelp();      m_commandMap.put(c.name, c);
		c = new CmdClear();     m_commandMap.put(c.name, c);
		c = new CmdNodes();     m_commandMap.put(c.name, c);
		c = new CmdChunkinfo(); m_commandMap.put(c.name, c);
		c = new CmdMigrate();   m_commandMap.put(c.name, c);
		c = new CmdQuit();      m_commandMap.put(c.name, c);
		c = new CmdPut();       m_commandMap.put(c.name, c);
	}
		
	
	public static void loop() {
		String command;
		String[] arguments;
		byte []arr;
		

		System.out.println("DXRAM shell v. 0.1");
		while (true) {

			arr=JNIconsole.readline();
			if (arr!=null) { 
				command = new String(arr,0,arr.length);
			//	System.out.println("Java: *"+command+"*");
				arguments = command.split(" ");

				Cmd c = m_commandMap.get(arguments[0]);
				if (c==null)
					System.out.println("error: unknown command");
				else {
					if (c.areParametersSane(arguments))
						c.execute(command);
				}
			}
		}
	}				

}
