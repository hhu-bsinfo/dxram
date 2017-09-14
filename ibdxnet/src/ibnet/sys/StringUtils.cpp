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

#include "StringUtils.h"

#include <sstream>

namespace ibnet {
namespace sys {

std::vector<std::string> StringUtils::Split(const std::string& text,
        const std::string& delimiter, bool ignoreEmptyTokens)
{
    std::string remaining = text;
    std::vector<std::string> result;
    std::string::size_type pos = remaining.find(delimiter);

    while (pos != std::string::npos)
    {
        // drop 0 sized strings
        std::string substr = remaining.substr(0, pos);
        if (substr.size() != 0 || !ignoreEmptyTokens)
        {
            result.push_back(substr);
        }

        remaining = remaining.substr(pos + 1);
        pos = remaining.find(delimiter);

        // using g++ the while condition was ignored for one
        // case though pos was set to npos (wtf)
        // adding this and everything's good again...
        if (pos == std::string::npos)
            break;
    }

    if (remaining.size() != 0)
        result.push_back(remaining);

    return result;
}

std::string StringUtils::ToHexString(uint64_t value, bool fillZeros, bool hexNumberIdent)
{
    return __ToHexString(value, fillZeros ? 16 : 0, hexNumberIdent);
}

std::string StringUtils::ToHexString(uint32_t value, bool fillZeros, bool hexNumberIdent)
{
    return __ToHexString(value, fillZeros ? 8 : 0, hexNumberIdent);
}

std::string StringUtils::ToHexString(uint16_t value, bool fillZeros, bool hexNumberIdent)
{
    return __ToHexString(value, fillZeros ? 4 : 0, hexNumberIdent);
}

std::string StringUtils::ToHexString(uint8_t value, bool fillZeros, bool hexNumberIdent)
{
    return __ToHexString(value, fillZeros ? 2 : 0, hexNumberIdent);
}

std::string StringUtils::__ToHexString(uint64_t value, uint32_t fillZerosCount, bool hexNumberIdent)
{
    std::ostringstream os;
    os << std::hex << value;

    std::string tmp;

    if (hexNumberIdent)
        tmp += "0x";
    if (fillZerosCount > 0)
    {
        for (size_t i = os.str().size(); i < fillZerosCount; i++)
            tmp += "0";
    }

    tmp += os.str();

    return tmp;
}

}
}