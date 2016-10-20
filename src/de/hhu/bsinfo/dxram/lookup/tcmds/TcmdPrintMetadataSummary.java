
package de.hhu.bsinfo.dxram.lookup.tcmds;

/**
 * Command to print a summary of a superpeer's metadata
 *
 * @author Kevin Beineke 12.10.2016
 */
public class TcmdPrintMetadataSummary {

	//	private static final Argument MS_ARG_NID =
	//			new Argument("nid", null, false, "NodeID of the superpeer");
	//
	//	@Override
	//	public String getName() {
	//
	//		return "metadata";
	//	}
	//
	//	@Override
	//	public String getDescription() {
	//
	//		return "prints a summary of the specified superpeer's metadata";
	//	}
	//
	//	@Override
	//	public void registerArguments(final ArgumentList p_arguments) {
	//
	//		p_arguments.setArgument(MS_ARG_NID);
	//	}
	//
	//	@Override
	//	public boolean execute(final ArgumentList p_arguments) {
	//		String summary;
	//		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
	//
	//		if (nid == null) {
	//			return false;
	//		}
	//
	//		LookupService lookupService = getTerminalDelegate().getDXRAMService(LookupService.class);
	//		if (lookupService == null) {
	//			return false;
	//		}
	//
	//		summary = lookupService.getMetadataSummary(nid);
	//		System.out.println("Metadata summary of " + NodeID.toHexString(nid) + ":\n" + summary);
	//
	//		return true;
	//	}

}
