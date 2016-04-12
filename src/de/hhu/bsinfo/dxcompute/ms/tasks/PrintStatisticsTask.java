
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

public abstract class PrintStatisticsTask extends AbstractTaskPayload {

	public PrintStatisticsTask(short p_typeId, short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	protected void printStatisticsToOutput(final PrintStream p_outputStream, final BootService p_bootService,
			final StatisticsService p_statisticsService) {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println(dateFormat.format(date));
		short nodeId = p_bootService.getNodeID();
		p_outputStream.println("NodeID: " + Integer.toHexString(nodeId) + " (" + nodeId + ")");
		p_outputStream.println("Role: " + p_bootService.getNodeRole(nodeId));
		p_outputStream.println("---------------------------------------------------------");

		ArrayList<StatisticsRecorder> recorders = p_statisticsService.getRecorders();
		for (StatisticsRecorder recorder : recorders) {
			p_outputStream.println(recorder.toString());
			p_outputStream.println("---------------------------------------------------------");
		}

		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
	}
}
