package de.uniduesseldorf.dxram.commands;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Arrays;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.CommandMessage;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

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
		
		c = new CmdHelp();     m_commandMap.put(c.name, c);
		c = new CmdClear();    m_commandMap.put(c.name, c);
		c = new CmdNodes();    m_commandMap.put(c.name, c);
		c = new CmdChunks();   m_commandMap.put(c.name, c);
		c = new CmdMigrate();  m_commandMap.put(c.name, c);
	}

	public static void loop() {
		Scanner scanner;
		String command;
		String[] arguments;

		System.out.println("DXRAM shell v. 0.1");

		scanner = new Scanner(System.in);
		while (true) {
			System.out.print(">");
			command = scanner.nextLine();
			arguments = command.split(" ");

			Cmd c = m_commandMap.get(arguments[0]);
			if (c==null)
				System.out.println("error: unknown command");
			else {
				if (c.areParametersSane(arguments))
					c.execute(command);
			}
		}
//		scanner.close();
	}
					
			
			
			/*
			switch (arguments[0]) {
			case "help":
			case "?":
				help();
				break;
				case "list":
					switch (arguments[1]) {
						case "nodes":
							break;
						default:
							System.out.println("error: wrong parameter");
							System.out.println("usage: list nodes");
							break;
					}
				break;
			case "migrate":
				// migrate: ChunkID, src, dest
				new CommandMessage(Short.parseShort(p_args[1]), type, p_args).send(m_network);
				break;
			default:
				System.out.println("error: unknown command");
				break;
			}

			*/
			
			/*if (command.equals("help")) {
				System.out.println("A command is built as follows: 'type arg1 arg2 ...'");
				System.out.println("Example: 'migrate CID FROM TO', whereas CID is the ChunkID of the Chunk "
						+ "that is migrated, FROM is the NodeID of the peer the Chunk resides now "
						+ "and TO is the NodeID of the peer the Chunk is sent to.");
			} else {
				arguments = command.split(" ");
				try {
					Core.execute(arguments[0], Arrays.copyOfRange(arguments, 1, arguments.length));
				} catch (final DXRAMException e) {
					scanner.close();
				}
			}
			
			
			
		}
		
	}*/
	
	
	
}
