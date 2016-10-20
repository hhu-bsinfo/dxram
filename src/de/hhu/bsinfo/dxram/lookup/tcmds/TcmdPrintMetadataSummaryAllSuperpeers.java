
package de.hhu.bsinfo.dxram.lookup.tcmds;

/**
 * Command to print a summary of a superpeer's metadata
 *
 * @author Kevin Beineke 12.10.2016
 */
public class TcmdPrintMetadataSummaryAllSuperpeers {

	//	@Override
	//	public String getName() {
	//
	//		return "metadataall";
	//	}
	//
	//	@Override
	//	public String getDescription() {
	//
	//		return "prints a summary of all superpeer's metadata";
	//	}
	//
	//	@Override
	//	public void registerArguments(final ArgumentList p_arguments) {}
	//
	//	@Override
	//	public boolean execute(final ArgumentList p_arguments) {
	//		String summary;
	//		ArrayList<Short> superpeers;
	//
	//		LookupService lookupService = getTerminalDelegate().getDXRAMService(LookupService.class);
	//		if (lookupService == null) {
	//			return false;
	//		}
	//
	//		superpeers = lookupService.getAllSuperpeers();
	//		for (short superpeer : superpeers) {
	//			summary = lookupService.getMetadataSummary(superpeer);
	//			System.out.println("Metadata summary of " + NodeID.toHexString(superpeer) + ":\n" + summary);
	//		}
	//
	//		return true;
	//	}

}
