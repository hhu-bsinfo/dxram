package de.hhu.bsinfo.dxcompute.stats;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.dxcompute.Task;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

public abstract class PrintStatisticsTask extends Task {

	public PrintStatisticsTask()
	{
		
	}
	
	protected void printStatisticsToOutput(final PrintStream p_outputStream)
	{		
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println(dateFormat.format(date));
		short nodeId = m_bootService.getNodeID();
		p_outputStream.println("NodeID: " + Integer.toHexString(nodeId).substring(4) + " (" + nodeId + ")");
		p_outputStream.println("Role: " + m_bootService.getNodeRole(nodeId));	
		p_outputStream.println("---------------------------------------------------------");
		
		ArrayList<StatisticsRecorder> recorders = m_statisticsService.getRecorders();
		for (StatisticsRecorder recorder : recorders)
		{
			p_outputStream.println(recorder.toString());
			p_outputStream.println("---------------------------------------------------------");
		}
		
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");		
	}
}
