package de.hhu.bsinfo.dxcompute;

/**
 * Dummy implementation of a task for testing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class NullTask extends Task {

	@Override
	protected boolean execute() {
		return true;
	}
}
