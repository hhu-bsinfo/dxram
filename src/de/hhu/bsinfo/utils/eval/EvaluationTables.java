/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.utils.eval;

import java.util.HashMap;
import java.util.Map;

/**
 * Have multiple tables recording data for an evaluation.
 * Refer to EvaluationTable as well.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class EvaluationTables {
    private EvaluationTable[] m_tables;
    private String[] m_tableNames;
    private Map<String, Integer> m_tableNameMappings = new HashMap<String, Integer>();

    /**
     * Constructor
     *
     * @param p_tables
     *         Number of tables to create.
     * @param p_rows
     *         Number of rows for the data (don't count descriptions).
     * @param p_columns
     *         Number of columns for the data (don't count descriptions).
     */
    public EvaluationTables(final int p_tables, final int p_rows, final int p_columns) {
        m_tables = new EvaluationTable[p_tables];
        for (int i = 0; i < m_tables.length; i++) {
            m_tables[i] = new EvaluationTable(p_rows, p_columns);
        }

        m_tableNames = new String[p_tables];
        for (int i = 0; i < m_tableNames.length; i++) {
            m_tableNames[i] = new String("Table " + i);
            m_tableNameMappings.put(m_tableNames[i], i);
        }
    }

    /**
     * Set the name for a table.
     *
     * @param p_table
     *         Table index.
     * @param p_name
     *         Name to set.
     */
    public void setTableName(final int p_table, final String p_name) {
        m_tableNames[p_table] = p_name;
        m_tableNameMappings.put(p_name, p_table);
    }

    /**
     * Sets the name of the top right corner cell intersecting
     * rows and columns. Sets identical names for all tables.
     *
     * @param p_name
     *         Name to set.
     */
    public void setIntersectTopCornerNames(final String p_name) {
        for (int i = 0; i < m_tables.length; i++) {
            m_tables[i].setIntersectTopCornerName(p_name);
        }
    }

    /**
     * Set the description/name of a row for all tables.
     *
     * @param p_row
     *         Row index.
     * @param p_name
     *         Row name.
     */
    public void setRowNames(final int p_row, final String p_name) {
        for (int i = 0; i < m_tables.length; i++) {
            m_tables[i].setRowName(p_row, p_name);
        }
    }

    /**
     * Set the description/name of a column for all tables.
     *
     * @param p_col
     *         Column index.
     * @param p_name
     *         Column name.
     */
    public void setColumnNames(final int p_col, final String p_name) {
        for (int i = 0; i < m_tables.length; i++) {
            m_tables[i].setColumnName(p_col, p_name);
        }
    }

    /**
     * Set data/value for a specific cell in a table.
     *
     * @param p_table
     *         Name of the table.
     * @param p_row
     *         Name of the row.
     * @param p_column
     *         Name of the column.
     * @param p_value
     *         Value/Data to set.
     */
    public void set(final String p_table, final String p_row, final String p_column, final String p_value) {
        int tableIdx = m_tableNameMappings.get(p_table);
        m_tables[tableIdx].set(p_row, p_column, p_value);
    }

    /**
     * Set data/value for a specific cell in a table.
     *
     * @param p_table
     *         Table index.
     * @param p_row
     *         Row index.
     * @param p_column
     *         Column index.
     * @param p_value
     *         Value/Data to set.
     */
    public void set(final int p_table, final int p_row, final int p_column, final String p_value) {
        m_tables[p_table].set(p_row, p_column, p_value);
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
     *         Description/Header for the table
     * @param p_delimiter
     *         Delimiter to use to separate cells
     * @return Csv formated table.
     */
    public String toCsv(final boolean p_description, final String p_delimiter) {
        String str = new String();

        for (int i = 0; i < m_tables.length; i++) {
            if (p_description) {
                str += m_tableNames[i] + "\n";
            }
            str += m_tables[i].toCsv(p_description, p_delimiter);
            str += "\n";
        }

        return str;
    }

    @Override public String toString() {
        return toCsv(false, ",");
    }
}
