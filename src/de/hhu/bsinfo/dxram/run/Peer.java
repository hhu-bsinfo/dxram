
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Run a DXRAM Peer instance.
 * @author Kevin Beineke 21.8.2015
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class Peer extends DXRAMMain {

	public static void main(final String[] args) {
		Main main = new Peer();
		main.run(args);
	}
	
	/**
	 * Creates an instance of Peer
	 */
	protected Peer() 
	{
		super(null, null, NodeRole.PEER);
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) {
		System.out.println("Peer started");
		
		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}
}
