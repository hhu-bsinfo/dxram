
package de.hhu.bsinfo.dxram.stats;

import java.util.Collection;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Exposing the component backend to the front with some
 * additional features like printing, filtering, ...
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class StatisticsService extends AbstractDXRAMService {

	/**
	 * Constructor
	 */
	public StatisticsService() {
		super("stats");
	}

	/**
	 * Print the statistics of all created recorders to the console.
	 */
	public void printStatistics() {
		StatisticsRecorderManager.getRecorders().forEach(System.out::println);
	}

	/**
	 * Print the statistics of a specific recorder to the console.
	 *
	 * @param p_className Fully qualified name of the class including package location (or relative to de.hhu.bsinfo)
	 */
	public void printStatistics(final String p_className) {
		Class<?> clss;
		try {
			clss = Class.forName(p_className);
		} catch (final ClassNotFoundException e) {
			// check again with longest common prefix of package names
			try {
				clss = Class.forName("de.hhu.bsinfo." + p_className);
			} catch (final ClassNotFoundException e1) {
				return;
			}
		}

		printStatistics(clss);
	}

	/**
	 * Print the statistics of a specific recorder to the console.
	 *
	 * @param p_class Class this recorder was created for.
	 */
	public void printStatistics(final Class<?> p_class) {
		StatisticsRecorder recorder = StatisticsRecorderManager.getRecorder(p_class);
		if (recorder != null) {
			System.out.println(recorder);
		}
	}

	/**
	 * Get all available statistic recorders
	 *
	 * @return Get recorders
	 */
	public Collection<StatisticsRecorder> getRecorders() {
		return StatisticsRecorderManager.getRecorders();
	}

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		// no dependencies
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}

}
