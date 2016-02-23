package de.hhu.bsinfo.dxcompute.stats;

public class PrintStatisticsToConsoleTask extends PrintStatisticsTask {

	@Override
	protected boolean execute() {
		printStatisticsToOutput(System.out);
		return true;
	}
}
