package de.hhu.bsinfo.dxcompute.old;

import de.hhu.bsinfo.dxcompute.ms.NullTask;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Dummy implementation of a pipeline for testing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class NullPipeline extends Pipeline {

	@Override
	public boolean setup(final ArgumentList p_arguments) {
		pushTask(new NullTask());
		pushTask(new NullTask());
		pushTask(new NullTask());

		return true;
	}
}
