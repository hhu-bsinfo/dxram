
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;

/**
 * Run a DXRAM Superpeer instance.
 * @author Kevin Beineke 21.08.2015
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class Superpeer extends DXRAMMain {

	public static void main(final String[] args) {
		Main main = new Superpeer();
		main.run(args);
	}
	
	/**
	 * Creates an instance of Superpeer
	 */
	protected Superpeer() 
	{
		super(null, null, NodeRole.SUPERPEER);
	}

	@Override
	protected int mainApplication(MainArguments p_arguments) {
		System.out.println("Superpeer started");
		
		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}
}
