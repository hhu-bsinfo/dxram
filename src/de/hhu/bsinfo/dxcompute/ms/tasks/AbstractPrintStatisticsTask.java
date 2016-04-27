
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
import de.hhu.bsinfo.menet.NodeID;

/**
 * Base class to print the statistics.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public abstract class AbstractPrintStatisticsTask extends AbstractTaskPayload {

	/**
	 * Constructor
	 * Expecting a default constructor for the sub classes extending this
	 * base class, otherwise the createInstance call won't work.
	 * Make sure to register each task payload implementation prior usage.
	 * @param p_typeId
	 *            Type id
	 * @param p_subtypeId
	 *            Subtype id
	 */
	public AbstractPrintStatisticsTask(final short p_typeId, final short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	/**
	 * Print the statistics to a stream.
	 * @param p_outputStream
	 *            Output stream to print to.
	 * @param p_bootService
	 *            BootService
	 * @param p_statisticsService
	 *            StatisticsService
	 */
	protected void printStatisticsToOutput(final PrintStream p_outputStream, final BootService p_bootService,
			final StatisticsService p_statisticsService) {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println(dateFormat.format(date));
		short nodeId = p_bootService.getNodeID();
		p_outputStream.println("NodeID: " + NodeID.toHexString(nodeId) + " (" + nodeId + ")");
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
