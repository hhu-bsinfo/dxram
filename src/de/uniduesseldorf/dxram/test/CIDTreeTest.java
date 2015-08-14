
package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.lookup.storage.CIDTreeOptimized;

/**
 * Test case for the CID data structures
 * @author Kevin Beineke
 *         03.01.2014
 */
public final class CIDTreeTest {

	// Constants
	private static final short ME = 12345;
	private static final short ORDER = 10;

	// Constructors
	/**
	 * Creates an instance of CIDTreeTest
	 */
	private CIDTreeTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 1) {
			evaluate(Integer.parseInt(p_arguments[1]));
		} else {
			System.out.println("Define number of elements");
		}
	}

	/**
	 * Evaluates chosen data structure
	 * @param p_numberOfEntries
	 *            The number of entries that have to be added
	 */
	public static void evaluate(final int p_numberOfEntries) {
		long addMe;

		short creator;
		long lower;
		long upper;
		long timeStart;
		long insert;
		long append;

		long rndm;

		CIDTreeOptimized cidTree;

		addMe = (long) ME << 48;

		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

		cidTree = new CIDTreeOptimized(ORDER);
		cidTree.initRange(0, ME, null);

		// Initialize ranges up to 2^22
		for (int i = 1; i <= Math.pow(2, 22); i++) {
			cidTree.initRange(i * 1000 - 1 + addMe, ME, new short[] {(short) (5 + i), -1, -1});
		}
		creator = 0;
		lower = 1;
		upper = (long) Math.pow(2, 22);
		System.out.println("Start test");
		timeStart = System.currentTimeMillis();

		for (int i = 1; i <= p_numberOfEntries; i++) {
			cidTree.migrateRange(i * 1000 + addMe, i * 1000 + 100 + addMe, (short) 2);
		}
		System.out.println("Appends finished");
		append = System.currentTimeMillis();
		for (int i = 1; i <= p_numberOfEntries; i++) {
			rndm = (long) ((upper - lower) * Math.random() + lower) + addMe;
			creator = (short) (1000 * Math.random());
			cidTree.migrateRange(rndm, rndm + 100, creator);
			if (creator != cidTree.getPrimaryPeer(rndm + 50)) {
				System.out.println("Wrong insertion, " + (rndm + 50 & 0x0000FFFFFFFFFFFFL) + ", " + creator);
			}
		}
		System.out.println("Inserts finished");
		insert = System.currentTimeMillis();
		for (int i = 1; i <= p_numberOfEntries; i++) {
			rndm = (long) (Math.pow(2, 22) * Math.random() + addMe);
			cidTree.getPrimaryPeer(rndm);
		}
		System.out.println("BTree: Append: " + (append - timeStart) + " ms, insert: " + (insert - append)
				+ "ms, get: " + (System.currentTimeMillis() - insert) + "ms");
		if (cidTree.validate()) {
			System.out.println("Number of entries in btree: " + cidTree.size() + ", btree is valid");
		} else {
			System.out.println("Number of entries in btree: " + cidTree.size() + ", btree is not valid");
		}

		/*
		 * Example to examine data structure manually
		 * cidTree.migrateObject(1111 + addMe, (short)1);
		 * for (int i = 1; i <= 10; i++) {
		 * cidTree.migrateObject((i * 1000) + addMe, (short)2);
		 * }
		 * System.out.println(cidTree.toString());
		 * cidTree.migrateObject(1001 + addMe, (short)2);
		 * cidTree.migrateObject(1003 + addMe, (short)5);
		 * cidTree.migrateObject(1002 + addMe, (short)2);
		 * cidTree.migrateObject(8000 + addMe, (short)5);
		 * System.out.println(cidTree.toString());
		 * cidTree.migrateObject(999 + addMe, (short)2);
		 * cidTree.migrateObject(995 + addMe, (short)2);
		 * cidTree.migrateObject(997 + addMe, (short)5);
		 * System.out.println(cidTree.toString());
		 * cidTree.migrateRange(500 + addMe, 1500 + addMe, (short)3);
		 * cidTree.migrateObject(800 + addMe, (short)10);
		 * cidTree.removeObject(500 + addMe);
		 * System.out.println(cidTree.toString());
		 * for (int i = 0; i < 11000; i += 10) {
		 * System.out.println(i + ": " + cidTree.getPrimaryPeer(i) + ", " + cidTree.getBackupPeers(i)[0]);
		 * }
		 */

	}
}
