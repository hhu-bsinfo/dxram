package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Simple test to verify if DXRAM starts and shuts down properly.
 * Run this as a peer, start one superpeer.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class SimpleDXRAMTest extends Main {

	private DXRAM m_dxram = null;

	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		Main main = new SimpleDXRAMTest();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	public SimpleDXRAMTest()
	{
		super("Simple test to verify if DXRAM starts and shuts down properly");
		
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf", true);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {

	}

	@Override
	protected int main(ArgumentList p_arguments) {
		return 0;
	}

}
