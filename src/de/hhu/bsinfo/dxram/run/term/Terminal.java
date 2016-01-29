package de.hhu.bsinfo.dxram.run.term;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.run.term.cmd.AbstractCmd;
import de.hhu.bsinfo.dxram.run.term.cmd.CmdClear;
import de.hhu.bsinfo.dxram.run.term.cmd.CmdQuit;
import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;

/**
 * Run a DXRAM Peer instance.
 * @author Kevin Beineke 21.8.2015
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class Terminal extends Main {

	private DXRAM m_dxram = null;
	
	private Map<String, AbstractCmd> m_commandMap = new HashMap<String, AbstractCmd>();
	
	public static void main(final String[] args) {
		Main main = new Terminal();
		main.run(args);
	}
	
	public Terminal() 
	{
		AbstractCmd c;

		c = new CmdClear();
		m_commandMap.put(c.getName(), c);
		c = new CmdQuit();
		m_commandMap.put(c.getName(), c);
	}

	@Override
	protected void registerDefaultProgramArguments(MainArguments p_arguments) {
	}

	@Override
	protected int main(MainArguments p_arguments) {
		m_dxram = new DXRAM();
		if (!m_dxram.initialize("config/dxram.conf", null, null, "Monitor", true)) {
			System.out.println("Failed starting monitor.");
			return -1;
		}

		System.out.println("Monitor started");

		// Wait a moment
		try {
			Thread.sleep(3000);
		} catch (final InterruptedException e) {}
		
		loop();
		return 0;
	}
	
	private void loop() 
	{
		String command;
		String[] arguments;
		byte[] arr;

		System.out.println("DXRAM terminal v. 0.1");

		while (true) {
			arr = JNIconsole.readline();
			if (arr != null) {
				command = new String(arr, 0, arr.length);
				arguments = command.split(" ");

				final AbstractCmd c = m_commandMap.get(arguments[0]);
				if (c == null) {
					System.out.println("error: unknown command");
				} else {
					if (c.areParametersSane(arguments)) {
						c.execute(command);
					} else {
						c.printUsage();
					}
				}
			}
		}
	}
}

