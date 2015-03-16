package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.lookup.storage.OIDTableTest;
import de.uniduesseldorf.dxram.core.lookup.storage.OIDTree;
import de.uniduesseldorf.dxram.core.lookup.storage.OIDTreeTest;

/**
 * Test case for the OID data structures
 * @author Kevin Beineke
 *         03.01.2014
 */
public final class OIDDataStructureTest {

	// Constants
	private static final short ME = 12345;
	private static final short ORDER = 10;

	// Constructors
	/**
	 * Creates an instance of OIDDataStructureTest
	 */
	private OIDDataStructureTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 2) {
			evaluate(Integer.parseInt(p_arguments[0]), Integer.parseInt(p_arguments[1]));
		} else {
			System.out.println("Define test mode and number of elements");
		}
	}

	/**
	 * Evaluates chosen data structure
	 * @param p_testMode
	 *            The chosen data structure (0 = B-Tree(final), 1 = B-Tree(experimental), other = ArrayList)
	 * @param p_numberOfEntries
	 *            The number of entries that have to be added
	 */
	public static void evaluate(final int p_testMode, final int p_numberOfEntries) {
		long addMe;

		short creator;
		long lower;
		long upper;
		long timeStart;
		long insert;
		long append;

		long rndm;

		OIDTree tableTree;
		OIDTreeTest tableTreeTest;
		OIDTableTest tableList;

		addMe = (long)ME << 48;

		if (0 == p_testMode) {
			// B-Tree (final)

			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.dxram"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			tableTree = new OIDTree(ORDER);
			tableTree.initRange(0, ME, null);

			// Initialize ranges up to 2^22
			for (int i = 1;i <= Math.pow(2, 22);i++) {
				tableTree.initRange(i * 1000 - 1 + addMe, ME, new short[] {(short)(5 + i), -1, -1});
			}
			creator = 0;
			lower = 1;
			upper = (long)Math.pow(2, 22);
			System.out.println("Start test");
			timeStart = System.currentTimeMillis();

			for (int i = 1;i <= p_numberOfEntries;i++) {
				tableTree.migrateRange(i * 1000 + addMe, i * 1000 + 100 + addMe, (short)2);
			}
			System.out.println("Appends finished");
			append = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)((upper - lower) * Math.random() + lower) + addMe;
				creator = (short)(1000 * Math.random());
				tableTree.migrateRange(rndm, rndm + 100, creator);
				if (creator != tableTree.getPrimaryPeer(rndm + 50)) {
					System.out.println("Wrong insertion, " + (rndm + 50 & 0x0000FFFFFFFFFFFFL) + ", " + creator);
				}
			}
			System.out.println("Inserts finished");
			insert = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)(Math.pow(2, 22) * Math.random() + addMe);
				tableTree.getPrimaryPeer(rndm);
			}
			System.out.println("BTree: Append: " + (append - timeStart) + " ms, insert: " + (insert - append)
					+ "ms, get: " + (System.currentTimeMillis() - insert) + "ms");
			if (tableTree.validate()) {
				System.out.println("Number of entries in btree: " + tableTree.size() + ", btree is valid");
			} else {
				System.out.println("Number of entries in btree: " + tableTree.size() + ", btree is not valid");
			}

		} else if (1 == p_testMode) {
			// B-Tree (experimental)
			tableTreeTest = new OIDTreeTest(ORDER, ME);

			creator = 0;
			lower = 1;
			upper = (long)Math.pow(2, 32);
			System.out.println("Start test");
			timeStart = System.currentTimeMillis();

			for (int i = 1;i <= p_numberOfEntries;i++) {
				tableTreeTest.migrateRange(i * 1000 + addMe, i * 1000 + 100 + addMe, (short)2);
			}
			System.out.println("Appends finished");
			append = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)((upper - lower) * Math.random() + lower) + addMe;
				creator = (short)(65535 * Math.random());
				tableTreeTest.migrateRange(rndm, rndm + 100, creator);
				if (creator != tableTreeTest.get(rndm + 50)) {
					System.out.println("Wrong insertion, " + ((rndm + 50) & 0x0000FFFFFFFFFFFFL) + ", " + creator);
				}
			}
			System.out.println("Inserts finished");
			insert = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)(Math.pow(2, 32) * Math.random() + addMe);
				tableTreeTest.get(rndm);
			}
			System.out.println("BTree: Append: " + (append - timeStart) + " ms, insert: " + (insert - append)
					+ "ms, get: " + (System.currentTimeMillis() - insert) + "ms");
			if (tableTreeTest.validate()) {
				System.out.println("Number of entries in btree: " + tableTreeTest.size() + ", btree is valid");
			} else {
				System.out.println("Number of entries in btree: " + tableTreeTest.size() + ", btree is not valid");
			}
		} else {
			// ArrayList (experimental)
			tableList = new OIDTableTest(ME);

			lower = 1;
			upper = (long)Math.pow(2, 32);
			System.out.println("Start test");
			timeStart = System.currentTimeMillis();

			for (int i = 1;i <= p_numberOfEntries;i++) {
				tableList.migrateRange(i * 1000 + addMe, i * 1000 + 100 + addMe, (short)2);
			}
			System.out.println("Appends finished");
			append = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)((upper - lower) * Math.random() + lower) + addMe;
				creator = (short)(65535 * Math.random());
				tableList.migrateRange(rndm, rndm + 100, creator);
				if (creator != tableList.get(rndm + 50)) {
					System.out.println("Wrong insertion, " + ((rndm + 50) & 0x0000FFFFFFFFFFFFL) + ", " + creator);
				}
			}
			System.out.println("Inserts finished");
			insert = System.currentTimeMillis();
			for (int i = 1;i <= p_numberOfEntries;i++) {
				rndm = (long)(Math.pow(2, 32) * Math.random() + addMe);
				tableList.get(rndm);
			}
			System.out.println("List: Append: " + (append - timeStart) + " ms, insert: " + (insert - append)
					+ "ms, get: " + (System.currentTimeMillis() - insert) + "ms");
			System.out.println("Number of entries in list: " + tableList.getTable().size());
		}

		/* Example to examine data structure manually
		 * tableTree.migrateObject(1111 + addMe, (short)1);
		 * for (int i = 1; i <= 10; i++) {
		 * tableTree.migrateObject((i * 1000) + addMe, (short)2);
		 * }
		 * System.out.println(tableTree.toString());
		 * tableTree.migrateObject(1001 + addMe, (short)2);
		 * tableTree.migrateObject(1003 + addMe, (short)5);
		 * tableTree.migrateObject(1002 + addMe, (short)2);
		 * tableTree.migrateObject(8000 + addMe, (short)5);
		 * System.out.println(tableTree.toString());
		 * tableTree.migrateObject(999 + addMe, (short)2);
		 * tableTree.migrateObject(995 + addMe, (short)2);
		 * tableTree.migrateObject(997 + addMe, (short)5);
		 * System.out.println(tableTree.toString());
		 * tableTree.migrateRange(500 + addMe, 1500 + addMe, (short)3);
		 * tableTree.migrateObject(800 + addMe, (short)10);
		 * tableTree.removeObject(500 + addMe);
		 * System.out.println(tableTree.toString());
		 * for (int i = 0; i < 11000; i += 10) {
		 * System.out.println(i + ": " + tableTree.getPrimaryPeer(i) + ", " + tableTree.getBackupPeers(i)[0]);
		 * }
		 */

	}
}
