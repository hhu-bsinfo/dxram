
package de.hhu.bsinfo.dxcompute.run;

import de.hhu.bsinfo.dxcompute.ms.ComputeMSBase;
import de.hhu.bsinfo.dxcompute.ms.ComputeMaster;
import de.hhu.bsinfo.dxcompute.ms.ComputeSlave;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.tasks.NullTaskPayload;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Main entry point to start a computing pipeline with DXCompute.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class DXComputeMasterSlave extends DXRAMMain {

	public static final Argument ARG_MASTER =
			new Argument("master", null, false, "True to run as master, false for slave");
	public static final Argument ARG_COMPUTE_GROUP_ID =
			new Argument("computeGroupId", null, false,
					"Compute group id allows to organize multiple master/slave groups. Master "
							+ "and slaves belonging to the same group must have matching IDs");

	/**
	 * Constructor
	 */
	protected DXComputeMasterSlave() {
		super("DXComputeMasterSlave main entry point.");
	}

	/**
	 * Main entry point.
	 * @param p_args
	 *            Console arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new DXComputeMasterSlave();
		main.run(p_args);
	}

	@Override
	protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
		super.registerDefaultProgramArguments(p_arguments);
		p_arguments.setArgument(ARG_MASTER);
		p_arguments.setArgument(ARG_COMPUTE_GROUP_ID);
	}

	@Override
	protected int mainApplication(final ArgumentList p_arguments) {
		// create pipeline using reflection
		boolean master = p_arguments.getArgument(ARG_MASTER).getValue(Boolean.class);
		int computeGroupId = p_arguments.getArgument(ARG_COMPUTE_GROUP_ID).getValue(Integer.class);

		System.out.println(">>> DXComputeMasterSlave started <<<");
		System.out.println("Master: " + master);
		System.out.println("Compute group id: " + computeGroupId);

		ComputeMSBase computeMs;
		if (master) {
			ComputeMaster computeMaster = new ComputeMaster(getDXRAM(), computeGroupId, false);

			// TODO have this as argument to test the master slave system
			for (int i = 0; i < 10; i++) {
				computeMaster.submitTask(new Task(new NullTaskPayload(), "Test" + i));
			}
			computeMs = computeMaster;
		} else {
			computeMs = new ComputeSlave(getDXRAM(), computeGroupId, false);
		}

		// TODO push task(s) via argument list to queue
		// use this for the terminal version as well

		computeMs.run();
		computeMs.shutdown();

		computeMs = null;

		return 0;
	}
}
