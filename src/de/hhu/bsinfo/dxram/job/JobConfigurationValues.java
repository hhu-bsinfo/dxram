package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for job package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class JobConfigurationValues {
	public static class Component {
		public static final Pair<String, Integer> NUM_WORKERS = new Pair<String, Integer>("NumWorkers", 4);
	}
}
