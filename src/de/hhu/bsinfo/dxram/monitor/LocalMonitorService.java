package de.hhu.bsinfo.dxram.monitor;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.utils.JNIconsole;

public class LocalMonitorService extends DXRAMService {

	private StatisticsComponent m_statistics = null;
	
	public void printStatisticsToConsole() {
		for (StatisticsRecorder recorder : m_statistics.getRecorders()) {
			System.out.println(recorder);
		}
	}
	
	public void run() {

	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_statistics = getComponent(StatisticsComponent.class);
		
		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}
}
