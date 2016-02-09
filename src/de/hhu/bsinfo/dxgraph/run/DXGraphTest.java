package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxgraph.DXGraph;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

public class DXGraphTest extends DXRAMMain {

	public static void main(final String[] args) {
		Main main = new DXGraphTest();
		main.run(args);
	}
	
	/**
	 * Creates an instance of Peer
	 */
	protected DXGraphTest() 
	{
		super(null, null, NodeRole.PEER);
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) {
		System.out.println("DXGraph Peer started");
		
		// Wait a moment
		try {
			Thread.sleep(3000);
		} catch (final InterruptedException e) {}
		
		// TODO have option to read pipeline to execute from command line arguments
		DXGraph dxgraph = new DXGraph(getDXRAM());
		dxgraph.executePipeline(new TaskPipelineTest());
		
		return 0;
	}
}
