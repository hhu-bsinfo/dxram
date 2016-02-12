package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.DXRAMService;

public class StatisticsService extends DXRAMService {

	private StatisticsComponent m_statistics = null;;
	
	public int createRecorder(final Class<?> p_class) {		
		return m_statistics.createRecorder(p_class);
	}
	
	public int createOperation(final int p_id, final String p_operationName)
	{
		return m_statistics.createOperation(p_id, p_operationName);
	}
	
	public void enter(final int p_recorderId, final int p_operationId) {
		m_statistics.enter(p_recorderId, p_operationId);
	}
	
	public void enter(final int p_recorderId, final int p_operationId, final long p_val) {
		m_statistics.enter(p_recorderId, p_operationId, p_val);
	}
	
	public void enter(final int p_recorderId, final int p_operationId, final double p_val) {
		m_statistics.enter(p_recorderId, p_operationId, p_val);
	}
	
	public void leave(final int p_recorderId, final int p_operationId) {
		m_statistics.leave(p_recorderId, p_operationId);
	}

	public ArrayList<StatisticsRecorder> getRecorders() {
		return m_statistics.getRecorders();
	}
	
	public StatisticsRecorder getRecorder(final Class<?> p_class) {
		return m_statistics.getRecorder(p_class);
	}
	
	public void printStatistics() {
		for (StatisticsRecorder recorder : m_statistics.getRecorders()) {
			System.out.println(recorder);
		}
	}
	
	public void printStatistics(final Class<?> p_class) {
		StatisticsRecorder recorder = m_statistics.getRecorder(p_class);
		if (recorder != null) {
			System.out.println(recorder);
		}
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
		m_statistics = null;
		return true;
	}

}
