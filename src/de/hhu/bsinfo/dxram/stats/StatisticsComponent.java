package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

public class StatisticsComponent extends DXRAMComponent {

	private LoggerComponent m_logger = null;
	
	private boolean m_enabledOverride = true;
	private Map<String, Boolean> m_disabledRecorders = new HashMap<String, Boolean>();
	private ArrayList<StatisticsRecorder> m_recorders = new ArrayList<StatisticsRecorder>();
	
	public StatisticsComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	public int createRecorder(final Class<?> p_class) {		
		StatisticsRecorder recorder = new StatisticsRecorder(m_recorders.size(), p_class.getSimpleName());
		m_recorders.add(recorder);
		
		// check blacklist
		if (m_disabledRecorders.get(p_class.getSimpleName()) != null) {
			recorder.setEnabled(false);
		}
		
		return recorder.getId();
	}
	
	public int createOperation(final int p_id, final String p_operationName)
	{
		StatisticsRecorder recorder = m_recorders.get(p_id);
		if (recorder == null) {
			m_logger.error(getClass(), "Cannot create operation " + p_operationName + " for recorder id " + p_id + " no such recorder registered.");
			return StatisticsRecorder.Operation.INVALID_ID;
		}
		
		return recorder.createOperation(p_operationName);
	}
	
	public void enter(final int p_recorderId, final int p_operationId) {
		if (!m_enabledOverride)
			return;
		
		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such recorder registered.");
			return;
		}
		
		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such operation registered.");
			return;
		}
		
		operation.enter();
	}
	
	public void enter(final int p_recorderId, final int p_operationId, final long p_val) {
		if (!m_enabledOverride)
			return;
		
		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such recorder registered.");
			return;
		}
		
		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such operation registered.");
			return;
		}
		
		operation.enter(p_val);
	}
	
	public void enter(final int p_recorderId, final int p_operationId, final double p_val) {
		if (!m_enabledOverride)
			return;
		
		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such recorder registered.");
			return;
		}
		
		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId + " no such operation registered.");
			return;
		}
		
		operation.enter(p_val);
	}
	
	public void leave(final int p_recorderId, final int p_operationId) {
		if (!m_enabledOverride)
			return;
		
		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			m_logger.error(getClass(), "Cannot leave operation " + p_operationId + " for recorder id " + p_recorderId + " no such recorder registered.");
			return;
		}
		
		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			m_logger.error(getClass(), "Cannot leave operation " + p_operationId + " for recorder id " + p_recorderId + " no such operation registered.");
			return;
		}
		
		operation.leave();
	}

	public ArrayList<StatisticsRecorder> getRecorders() {
		return m_recorders;
	}
	
	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
		p_settings.setDefaultValue(StatisticsConfigurationValues.Component.RECORD);
	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		
		m_enabledOverride = p_settings.getValue(StatisticsConfigurationValues.Component.RECORD);
		
		m_logger.info(getClass(), "Recording of statistics enabled (global override): " + m_enabledOverride);
		
		// read further entries, which can disable single categories (optional)
		Map<Integer, String> catDisabled = p_settings.getValues("/CategoryDisabled", String.class);
		if (catDisabled != null)
		{
			for (Entry<Integer, String> entry : catDisabled.entrySet()) {
				m_disabledRecorders.put(entry.getValue(), true);
				m_logger.debug(getClass(), "Recorder " + entry.getValue() + " disabled.");
			}
		}
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
