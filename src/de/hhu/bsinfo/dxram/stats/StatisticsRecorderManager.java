package de.hhu.bsinfo.dxram.stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by nothaas on 10/27/16.
 */
public class StatisticsRecorderManager {

	private static ReentrantLock ms_mapLock = new ReentrantLock(false);
	private static Map<String, StatisticsRecorder> ms_recorders = new HashMap<>();

	/**
	 * Static class
	 */
	private StatisticsRecorderManager() {

	}

	public static StatisticsRecorder getRecorder(final Class<?> p_class) {

		StatisticsRecorder recorder = ms_recorders.get(p_class.getSimpleName());
		if (recorder == null) {
			ms_mapLock.lock();
			recorder = ms_recorders.get(p_class.getSimpleName());
			if (recorder == null) {
				recorder = new StatisticsRecorder(p_class.getSimpleName());
				ms_recorders.put(p_class.getSimpleName(), recorder);
			}
			ms_mapLock.unlock();
		}

		return recorder;
	}

	public static Collection<StatisticsRecorder> getRecorders() {
		return ms_recorders.values();
	}

	public static StatisticsOperation getOperation(final Class<?> p_class, final String p_operationName) {

		return getRecorder(p_class).getOperation(p_operationName);
	}
}
