/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxterm;

import java.io.Serializable;

public class TerminalStdoutData implements Serializable {
    private String m_text = "";
    private TerminalColor m_color = TerminalColor.DEFAULT;
    private TerminalColor m_background = TerminalColor.DEFAULT;
    private TerminalStyle m_style = TerminalStyle.NORMAL;

    public TerminalStdoutData() {

    }

    public TerminalStdoutData(final String p_text) {
        m_text = p_text;
    }

    public TerminalStdoutData(final String p_text, final TerminalColor p_color, final TerminalColor p_background, final TerminalStyle p_style) {
        m_text = p_text;
        m_color = p_color;
        m_background = p_background;
        m_style = p_style;
    }

    public String getText() {
        return m_text;
    }

    public TerminalColor getColor() {
        return m_color;
    }

    public TerminalColor getBackground() {
        return m_background;
    }

    public TerminalStyle getStyle() {
        return m_style;
    }
}
