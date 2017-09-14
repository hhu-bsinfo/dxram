/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
 * Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

#ifndef IBNET_SYS_STRINGUTILS_H
#define IBNET_SYS_STRINGUTILS_H

#include <string>
#include <vector>

namespace ibnet {
namespace sys {

/**
 * Collection of utility functions for string related operations
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class StringUtils
{
public:
    /**
     * Split a string
     *
     * @param text String to split
     * @param delimiter One or multiple delimiters
     * @param ignoreEmptyTokens Filter empty tokens
     * @return Vector with tokens
     */
    static std::vector<std::string> Split(const std::string& text,
            const std::string& delimiter, bool ignoreEmptyTokens = true);

    /**
     * Convert a string to a hex string representation, e.g.
     * 0x1234
     *
     * @param value Value to convert
     * @param fillZeros Fill with leading 0s
     * @param hexNumberIdent Add hex number ident "0x"
     * @return Hex string representation of value
     */
    static std::string ToHexString(uint64_t value, bool fillZeros = true,
                                   bool hexNumberIdent = true);

    /**
     * Convert a string to a hex string representation, e.g.
     * 0x1234
     *
     * @param value Value to convert
     * @param fillZeros Fill with leading 0s
     * @param hexNumberIdent Add hex number ident "0x"
     * @return Hex string representation of value
     */
    static std::string ToHexString(uint32_t value, bool fillZeros = true,
                                   bool hexNumberIdent = true);

    /**
     * Convert a string to a hex string representation, e.g.
     * 0x1234
     *
     * @param value Value to convert
     * @param fillZeros Fill with leading 0s
     * @param hexNumberIdent Add hex number ident "0x"
     * @return Hex string representation of value
     */
    static std::string ToHexString(uint16_t value, bool fillZeros = true,
                                   bool hexNumberIdent = true);

    /**
     * Convert a string to a hex string representation, e.g.
     * 0x1234
     *
     * @param value Value to convert
     * @param fillZeros Fill with leading 0s
     * @param hexNumberIdent Add hex number ident "0x"
     * @return Hex string representation of value
     */
    static std::string ToHexString(uint8_t value, bool fillZeros = true,
                                   bool hexNumberIdent = true);

private:
    StringUtils(void) {};
    ~StringUtils(void) {};

    static std::string __ToHexString(uint64_t value, uint32_t fillZerosCount, bool hexNumberIdent);
};

}
}

#endif //IBNET_SYS_STRINGUTILS_H
