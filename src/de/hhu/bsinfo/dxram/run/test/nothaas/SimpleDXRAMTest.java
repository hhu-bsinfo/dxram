package de.hhu.bsinfo.dxram.run.test.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

// simple test to verify if dxram starts and shuts down properly
// run this as a peer, start one superpeer
public class SimpleDXRAMTest extends Main {

	private DXRAM m_dxram = null;

	public static void main(final String[] args) {
		Main main = new SimpleDXRAMTest();
		main.run(args);
	}
	
	public SimpleDXRAMTest()
	{
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
