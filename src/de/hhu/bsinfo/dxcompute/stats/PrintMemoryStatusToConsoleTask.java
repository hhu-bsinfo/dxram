package de.hhu.bsinfo.dxcompute.stats;

public class PrintMemoryStatusToConsoleTask extends PrintMemoryStatusTask {

	@Override
	protected boolean execute() {
		printMemoryStatusToOutput(System.out);
		return true;
	}
}
