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

package de.hhu.bsinfo.dxutils.eval;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple table for recording data for an evaluation and
 * printing it in a csv formated manner for easy copy/pasting
 * to other programs.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class EvaluationTable {
    private String m_intersectTopCornerName = "";
    private String[][] m_tableValues;
    private String[] m_rowNames;
    private Map<String, Integer> m_rowNameMapping = new HashMap<String, Integer>();
    private String[] m_columnNames;
    private Map<String, Integer> m_columnNameMapping = new HashMap<String, Integer>();

    /**
     * Constructor
     *
     * @param p_rows
     *     Number of rows for the data (don't count descriptions).
     * @param p_columns
     *     Number of columns for the data (don't count descriptions).
     */
    public EvaluationTable(final int p_rows, final int p_columns) {
        m_tableValues = new String[p_rows][p_columns];
        for (int i = 0; i < m_tableValues.length; i++) {
            for (int j = 0; j < m_tableValues[i].length; j++) {
                m_tableValues[i][j] = "";
            }
        }

        m_rowNames = new String[p_rows];
        for (int i = 0; i < m_rowNames.length; i++) {
            m_rowNames[i] = "Row " + i;
            m_rowNameMapping.put(m_rowNames[i], i);
        }

        m_columnNames = new String[p_columns];
        for (int i = 0; i < m_columnNames.length; i++) {
            m_columnNames[i] = "Column " + i;
            m_columnNameMapping.put(m_columnNames[i], i);
        }
    }

    /**
     * Sets the name of the top right corner cell intersecting
     * rows and columns.
     *
     * @param p_name
     *     Name to set.
     */
    public void setIntersectTopCornerName(final String p_name) {
        m_intersectTopCornerName = p_name;
    }

    /**
     * Set the description/name of a row.
     *
     * @param p_row
     *     Row index.
     * @param p_name
     *     Row name.
     */
    public void setRowName(final int p_row, final String p_name) {
        m_rowNames[p_row] = p_name;
        m_rowNameMapping.put(p_name, p_row);
    }

    /**
     * Set the description/name of a column.
     *
     * @param p_col
     *     Column index.
     * @param p_name
     *     Column name.
     */
    public void setColumnName(final int p_col, final String p_name) {
        m_columnNames[p_col] = p_name;
        m_columnNameMapping.put(p_name, p_col);
    }

    /**
     * Set data/value for a specific cell.
     *
     * @param p_row
     *     Name of the row.
     * @param p_column
     *     Name of the column.
     * @param p_value
     *     Value/Data to set.
     */
    public void set(final String p_row, final String p_column, final String p_value) {
        int rowIdx = m_rowNameMapping.get(p_row);
        int colIdx = m_columnNameMapping.get(p_column);
        m_tableValues[rowIdx][colIdx] = p_value;
    }

    /**
     * Set data/value for a specific cell.
     *
     * @param p_row
     *     Row index.
     * @param p_column
     *     Column index.
     * @param p_value
     *     Value/Data to set.
     */
    public void set(final int p_row, final int p_column, final String p_value) {
        m_tableValues[p_row][p_column] = p_value;
    }

    /**
     * Create a string containing the table as formated csv separated by ;
     *
     * @return Csv formated table.
     */
    public String toCsv() {
        return toCsv(true, ";");
    }

    /**
     * Create a string containing the table as formated csv
     *
     * @param p_description
     *     Description/Header for the table
     * @param p_delimiter
     *     Delimiter to use to separate cells
     * @return Csv formated table.
     */
    public String toCsv(final boolean p_description, final String p_delimiter) {
        String str = "";

        if (p_description) {
            str += m_intersectTopCornerName + p_delimiter;

            for (int i = 0; i < m_columnNames.length; i++) {
                str += m_columnNames[i] + p_delimiter;
            }

            str += "\n";
        }

        for (int i = 0; i < m_tableValues.length; i++) {
            if (p_description) {
                str += m_rowNames[i] + p_delimiter;
            }
            for (int j = 0; j < m_tableValues[i].length; j++) {
                str += m_tableValues[i][j] + p_delimiter;
            }
            str += "\n";
        }

        return str;
    }

    @Override
    public String toString() {
        return toCsv(false, ",");
    }
}
