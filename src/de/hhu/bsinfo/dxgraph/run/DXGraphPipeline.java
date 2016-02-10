package de.hhu.bsinfo.dxgraph.run;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import de.hhu.bsinfo.dxgraph.DXGraph;
import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;
import de.hhu.bsinfo.dxgraph.pipe.NullPipeline;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

public class DXGraphPipeline extends DXRAMMain {

	public static final Pair<String, String> ARG_PIPELINE = new Pair<String, String>("Pipeline", NullPipeline.class.getName());
	
	public static void main(final String[] args) {
		Main main = new DXGraphPipeline();
		main.run(args);
	}
	
	/**
	 * Creates an instance of Peer
	 */
	protected DXGraphPipeline() 
	{
		super(null, null, NodeRole.PEER);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		super.registerDefaultProgramArguments(p_arguments);
		p_arguments.setArgument(ARG_PIPELINE);
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) {
		System.out.println("DXGraph Peer started");
		
		// Wait a moment
		try {
			Thread.sleep(3000);
		} catch (final InterruptedException e) {}

		// create pipeline using reflection
		String pipelineName = p_arguments.getArgument(ARG_PIPELINE);
		System.out.println("Executing pipeline: " + pipelineName);
		
		GraphTaskPipeline pipeline = getPipeline(pipelineName);
		if (pipeline == null) {
			return -1;
		}
		
		DXGraph dxgraph = new DXGraph(getDXRAM());
		if (!dxgraph.executePipeline(pipeline)) {
			return -2;
		}
		
		return 0;
	}
	
	private GraphTaskPipeline getPipeline(final String p_name) {
		Class<?> clazz = null;
		try {
			clazz = Class.forName(p_name);
		} catch (ClassNotFoundException e) {
			System.out.println("Could not find class " + p_name + " to create pipeline instance.");
			return null;
		}
		
		Constructor<?> ctor = null;
		
		try {
			ctor = clazz.getConstructor();
		} catch (NoSuchMethodException | SecurityException e1) {
			System.out.println("Could not get default constructor of pipeline " + p_name + ".");
			return null;
		}
		
		GraphTaskPipeline pipeline = null;
		try {
			pipeline = (GraphTaskPipeline) ctor.newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			System.out.println("Could not create instance of pipeline " + p_name + ".");
		}
		
		return pipeline;
	}
}
