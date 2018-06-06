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
 * This class lists all nics.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public final class NICLister {

    private static ArrayList<String> ms_nics = new ArrayList<>();

    private NICLister() {
    }

    private static void updateList() {
        List<String> out = null;
        try {
            out = Files.readAllLines(Paths.get("/proc/net/dev"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert out != null;
        out.remove(0);
        out.remove(0);

        for (String s : out) {
            //if(s.equals(" "))break;
            String tmp = s.substring(0, s.indexOf(':'));
            ms_nics.add(ltrim(tmp));
        }
    }

    public static ArrayList<String> getNICList() {
        updateList();
        return ms_nics;
    }

    //own trim function
    private static String ltrim(final String p_s) {
        int i = 0;
        while (i < p_s.length() && Character.isWhitespace(p_s.charAt(i))) {
            i++;
        }
        return p_s.substring(i);
    }
}
