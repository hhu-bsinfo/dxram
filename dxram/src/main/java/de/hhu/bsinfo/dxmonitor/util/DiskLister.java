/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */


package de.hhu.bsinfo.dxmonitor.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class lists all disks.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public final class DiskLister {

    private static ArrayList<String> ms_disks = new ArrayList<>();

    /**
     * Initializes the ArrayList.
     */
    private DiskLister() {
    }

    /**
     * Updates the list.
     */
    private static void updateList() {
        List<String> out = null;
        try {
            out = Files.readAllLines(Paths.get("/proc/partitions"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert out != null;
        out.remove(0);
        out.remove(0);

        for (String s : out) {
            //if(s.equals(" "))break;
            ms_disks.add(s.substring(s.lastIndexOf(' ')).trim());
        }
    }

    /**
     * Returns a list of disk names.
     *
     * @return list of disk names
     */
    public static ArrayList<String> getDiskList() {
        updateList();
        return ms_disks;
    }

}
