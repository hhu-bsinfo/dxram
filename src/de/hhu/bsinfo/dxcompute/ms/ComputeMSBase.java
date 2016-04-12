
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.ExecuteTaskResponse;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinRequest;
import de.hhu.bsinfo.dxcompute.ms.messages.SlaveJoinResponse;
import de.hhu.bsinfo.dxcompute.ms.tasks.MasterSlaveTaskPayloads;
import de.hhu.bsinfo.dxcompute.ms.tasks.NullTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.tasks.WaitTaskPayload;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;

public abstract class ComputeMSBase extends Thread {
	protected DXRAM m_dxram;
	protected LoggerService m_loggerService;
	protected NetworkService m_networkService;
	protected BootService m_bootService;
	protected NameserviceService m_nameserviceService;

	protected enum State {
		STATE_SETUP,
		STATE_IDLE,
		STATE_EXECUTE,
	}

	protected volatile State m_state = State.STATE_SETUP;
	protected int m_computeGroupId = -1;
	protected String m_nameserviceMasterNodeIdKey;

	public ComputeMSBase(final DXRAM p_dxram, final int p_computeGroupId) {
		m_dxram = p_dxram;
		m_loggerService = m_dxram.getService(LoggerService.class);
		m_networkService = m_dxram.getService(NetworkService.class);
		m_bootService = m_dxram.getService(BootService.class);
		m_nameserviceService = m_dxram.getService(NameserviceService.class);

		m_computeGroupId = p_computeGroupId;
		assert m_computeGroupId >= 0 && m_computeGroupId <= 99;
		m_nameserviceMasterNodeIdKey = new String("MAS" + m_computeGroupId);

		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST, SlaveJoinRequest.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE, SlaveJoinResponse.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST, ExecuteTaskRequest.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_RESPONSE, ExecuteTaskResponse.class);

		registerTaskPayloads();
	}

	public int getComputeGroupId() {
		return m_computeGroupId;
	}

	@Override
	public abstract void run();

	public abstract void shutdown();

	private void registerTaskPayloads() {
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_NULL_TASK,
				NullTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(MasterSlaveTaskPayloads.TYPE,
				MasterSlaveTaskPayloads.SUBTYPE_WAIT_TASK,
				WaitTaskPayload.class);
	}
}
