'use strict';

const StringParser = require('./string');

const Long = require('long');

const INVALID_DATE = new Date(NaN);

// this is nearly duplicate of previous function so generated code is not slower
// due to "if (dateStrings)" branching
const pad = '000000000000';
function leftPad(num, value) {
  const s = value.toString();
  // if we don't need to pad
  if (s.length >= num) {
    return s;
  }
  return (pad + s).slice(-num);
}

// The whole reason parse* function below exist
// is because String creation is relatively expensive (at least with V8), and if we have
// a buffer with "12345" content ideally we would like to bypass intermediate
// "12345" string creation and directly build 12345 number out of
// <Buffer 31 32 33 34 35> data.
// In my benchmarks the difference is ~25M 8-digit numbers per second vs
// 4.5 M using Number(packet.readLengthCodedString())
// not used when size is close to max precision as series of *10 accumulate error
// and approximate result mihgt be diffreent from (approximate as well) Number(bigNumStringValue))
// In the futire node version if speed difference is smaller parse* functions might be removed
// don't consider them as Packet public API

const minus = '-'.charCodeAt(0);
const plus = '+'.charCodeAt(0);

// Handle E notation
const dot = '.'.charCodeAt(0);
const exponent = 'e'.charCodeAt(0);
const exponentCapital = 'E'.charCodeAt(0);

const PacketParser = {
  encode: StringParser.encode,
  decode: StringParser.decode,

  int8(buffer, offset) {
    return buffer.readInt8(offset || 0);
  },
  uint8(buffer, offset) {
    return buffer[offset || 0];
  },
  int16(buffer, offset) {
    return buffer.readInt16LE(offset || 0);
  },
  uint16(buffer, offset) {
    return buffer.readUInt16LE(offset || 0);
  },
  int32(buffer, offset) {
    return buffer.readInt32LE(offset || 0);
  },
  uint32(buffer, offset) {
    return buffer.readUInt32LE(offset || 0);
  },

  uint64Long(buffer, offset) {
    if (typeof offset === 'undefined') {
      offset = 0;
    }
    const word0 = buffer.readInt32LE(offset);
    const word1 = buffer.readInt32LE(4 + offset);
    return new Long(word0, word1, true);
  },

  int64Long(buffer, offset) {
    if (typeof offset === 'undefined') {
      offset = 0;
    }
    const word0 = buffer.readInt32LE(offset);
    const word1 = buffer.readInt32LE(4 + offset);
    return new Long(word0, word1, false);
  },

  uint64Number(buffer, offset) {
    return PacketParser.uint64Long(buffer, offset).toNumber();
  },

  int64Number(buffer, offset) {
    if (typeof offset === 'undefined') {
      offset = 0;
    }
    const word0 = buffer.readInt32LE(offset);
    const word1 = buffer.readInt32LE(4 + offset);
    if (!(word1 & 0x80000000)) {
      // Positive integers: calculate value without creating Long
      return word0 + 0x100000000 * word1;
    }
    const l = new Long(word0, word1, false);
    return l.toNumber();
  },

  uint64String(buffer, offset) {
    return PacketParser.uint64Long(buffer, offset).toString();
  },

  int64String(buffer, offset) {
    return PacketParser.int64Long(buffer, offset).toString();
  },

  uint64NumberIfPossible(buffer, offset) {
    const long = PacketParser.uint64Long(buffer, offset).toString();
    const resNumber = long.toNumber();
    const resString = long.toString();

    return resNumber.toString() === resString ? resNumber : resString;
  },

  int64NumberIfPossible(buffer, offset) {
    const long = PacketParser.int64Long(buffer, offset).toString();
    const resNumber = long.toNumber();
    const resString = long.toString();

    return resNumber.toString() === resString ? resNumber : resString;
  },

  float32(buffer, offset) {
    return buffer.readFloatLE(offset || 0);
  },

  float64(buffer, offset) {
    return buffer.readDoubleLE(offset || 0);
  },

  floatAscii(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }

    const end = offset + length;
    let result = 0;
    let factor = 1;
    let pastDot = false;
    let charCode = 0;
    if (length === 0) {
      return NaN; // TODO: assert? exception?
    }
    if (buffer[offset] === minus) {
      offset++;
      factor = -1;
    }
    if (buffer[offset] === plus) {
      offset++; // just ignore
    }
    while (offset < end) {
      charCode = buffer[offset];
      if (charCode === dot) {
        pastDot = true;
        offset++;
      } else if (charCode === exponent || charCode === exponentCapital) {
        offset++;
        const exponentValue = Number(buffer.toString('ascii', offset, end));
        return (result / factor) * Math.pow(10, exponentValue);
      } else {
        result *= 10;
        result += buffer[offset] - 0x30;
        offset++;
        if (pastDot) {
          factor = factor * 10;
        }
      }
    }
    return result / factor;
  },

  // Read MySQL date from binary buffer into Date
  dateTime(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }
    let y = 0;
    let m = 0;
    let d = 0;
    let H = 0;
    let M = 0;
    let S = 0;
    let ms = 0;
    if (length > 3) {
      y = PacketParser.uint16(buffer, offset);
      m = PacketParser.uint8(buffer, 2 + offset);
      d = PacketParser.uint8(buffer, 3 + offset);

      if (length > 6) {
        H = PacketParser.uint8(buffer, 4 + offset);
        M = PacketParser.uint8(buffer, 5 + offset);
        S = PacketParser.uint8(buffer, 6 + offset);

        if (length > 10) {
          ms = PacketParser.uint32(buffer, 7 + offset) / 1000;
        }
      }
    }
    return y || m || d || H || M || S || ms
      ? new Date(y, m - 1, d, H, M, S, ms)
      : INVALID_DATE;
  },

  // Read MySQL date from binary buffer as String
  dateTimeString(buffer, offset, length, decimals) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }
    let y = 0;
    let m = 0;
    let d = 0;
    let H = 0;
    let M = 0;
    let S = 0;
    let microseconds = 0;
    let str;
    if (length > 3) {
      y = PacketParser.uint16(buffer, offset);
      m = PacketParser.uint8(buffer, 2 + offset);
      d = PacketParser.uint8(buffer, 4 + offset);
      str = `${leftPad(4, y)}-${leftPad(2, m)}-${leftPad(2, d)}`;

      if (length > 6) {
        H = PacketParser.uint8(buffer, 4 + offset);
        M = PacketParser.uint8(buffer, 5 + offset);
        S = PacketParser.uint8(buffer, 6 + offset);
        str += ` ${leftPad(2, H)}:${leftPad(2, M)}:${leftPad(2, S)}`;

        // TODO AMH: Check if this functions correctly if microseconds = 0 but decimals > 0
        if (length > 10) {
          microseconds = PacketParser.uint32(buffer, 7 + offset);
          microseconds = leftPad(6, microseconds);
          str += '.';
          if (decimals) {
            if (microseconds.length > decimals) {
              microseconds = microseconds.substring(0, decimals); // rounding is done at the MySQL side, only 0 are here
            }
          }
          str += microseconds;
        }
      }
    }
    return str;
  },

  // Read MySQL date from ASCII buffer as Date
  dateAscii(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }
    let y = 0;
    let m = 0;
    let d = 0;
    let H = 0;
    let M = 0;
    let S = 0;
    let ms = 0;
    if (length > 9) {
      y = PacketParser.intAsciiSmall(buffer, offset, 4);
      m = PacketParser.intAsciiSmall(buffer, 5 + offset, 2);
      d = PacketParser.intAsciiSmall(buffer, 8 + offset, 2);

      if (length > 18) {
        H = PacketParser.intAsciiSmall(buffer, 11 + offset, 2);
        M = PacketParser.intAsciiSmall(buffer, 14 + offset, 2);
        S = PacketParser.intAsciiSmall(buffer, 17 + offset, 2);

        if (length > 20) {
          const microseconds = PacketParser.intAsciiSmall(
            buffer,
            20 + offset,
            length - 20
          );
          ms = leftPad(6, microseconds) / 1000;
        }
      }
    }
    return new Date(y, m, d, H, M, S, ms);
  },

  // Read MySQL time from binary buffer as String
  timeString(buffer, offset, length) {
    return PacketParser._time(String, buffer, offset, length);
  },
  // timeMilliseconds(buffer, offset, length) {
  //   return PacketParser._time(Number, buffer, offset, length);
  // },
  _time(conversion, buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }
    // 'isNegative' flag byte
    const sign = length && PacketParser.uint8(buffer, offset) ? -1 : 1;
    let d = 0;
    let H = 0;
    let M = 0;
    let S = 0;
    let microseconds = 0;
    if (length > 7) {
      d = PacketParser.uint32(buffer, 1 + offset);
      H = PacketParser.uint8(buffer, 5 + offset);
      M = PacketParser.uint8(buffer, 6 + offset);
      S = PacketParser.uint8(buffer, 7 + offset);

      if (length > 11) {
        microseconds = PacketParser.uint32(buffer, 8 + offset);
      }
    }
    if (conversion === String) {
      // TODO AMH: Check if this functions correctly if microseconds = 0 but decimals > 0
      return (
        (sign === -1 ? '-' : '') +
        [d ? d * 24 + H : H, leftPad(2, M), leftPad(2, S)].join(':') +
        (microseconds ? `.${leftPad(6, microseconds)}` : '')
      );
    }
    if (conversion === Number) {
      H += d * 24;
      M += H * 60;
      S += M * 60;
      let ms = S * 1000;
      ms += (microseconds / 1000) | 0;
      return sign * ms;
    }
    // if (conversion === Date.UTC) {
    //   return new Date(Date.UTC(0, 0, sign * d, sign * H, sign * M, sign * S, sign * ms));
    // }
    // if (conversion === Date) {
    //   return new Date(0, 0, sign * d, sign * H, sign * M, sign * S, sign * ms);
    // }
    throw new Error('Invalid conversion in PacketParser._time');
  },

  intAscii(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }

    if (!length) {
      return NaN; // TODO: assert? exception?
    }

    const end = offset + length;
    let numDigits = length;
    let position = offset;
    const isNegative = buffer[offset] === minus;
    if (isNegative) {
      position++;
      numDigits--;
    }
    if (buffer[position] === plus) {
      position++; // just ignore
      numDigits--;
    }
    let str;
    let result = 0;

    // Max precise int is 9007199254740992 (length 16)
    // Return values outside of precise Number range as String
    if (numDigits > 16) {
      return buffer.toString('ascii', offset, length);
    }

    // All out-of-range length 16 integers begin with 9
    if (numDigits === 16 && buffer[position] === 0x39) {
      str = buffer.toString('ascii', offset, length);
      result = parseInt(str, 10);
      // Return Number if value can be exactly represented
      return result.toFixed() === str ? result : str;
    }

    while (position < end) {
      result *= 10;
      result += buffer[position] - 0x30;
      position++;
    }
    return isNegative ? -result : result;
  },

  // Note that if value of buffer is bigger than MAX_SAFE_INTEGER
  // ( or smaller than MIN_SAFE_INTEGER ) the intAsciiSmall result might be
  // different from what you would get from Number(buffer)
  // String(buffer) <> String(Number(buffer)) <> buffer
  intAsciiSmall(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }
    let result = 0;
    const end = offset + length;
    if (!length) {
      return NaN; // TODO: assert? exception?
    }
    const isNegative = buffer[offset] === minus;
    if (isNegative) {
      offset++;
    }
    if (buffer[offset] === plus) {
      offset++; // just ignore
    }
    while (offset < end) {
      result *= 10;
      result += buffer[offset] - 0x30;
      offset++;
    }
    return isNegative ? -result : result;
  },

  // copy-paste from https://github.com/mysqljs/mysql/blob/master/lib/protocol/Parser.js
  // Refactored to accept parameters.
  geometry(buffer, offset, length) {
    if (typeof offset === 'undefined') {
      offset = 0;
      length = buffer.length;
    } else if (typeof length === 'undefined') {
      length = buffer.length - offset;
    }

    if (buffer === null || !length || length < 4) {
      return null;
    }

    // Skip first 4 bytes
    offset += 4;

    function parseGeometry() {
      let x, y, i, j, numPoints, line;
      let result = null;
      const byteOrder = buffer.readUInt8(offset);
      offset += 1;
      const wkbType = byteOrder
        ? buffer.readUInt32LE(offset)
        : buffer.readUInt32BE(offset);
      offset += 4;
      switch (wkbType) {
        case 1: // WKBPoint
          x = byteOrder
            ? buffer.readDoubleLE(offset)
            : buffer.readDoubleBE(offset);
          offset += 8;
          y = byteOrder
            ? buffer.readDoubleLE(offset)
            : buffer.readDoubleBE(offset);
          offset += 8;
          result = { x: x, y: y };
          break;
        case 2: // WKBLineString
          numPoints = byteOrder
            ? buffer.readUInt32LE(offset)
            : buffer.readUInt32BE(offset);
          offset += 4;
          result = [];
          for (i = numPoints; i > 0; i--) {
            x = byteOrder
              ? buffer.readDoubleLE(offset)
              : buffer.readDoubleBE(offset);
            offset += 8;
            y = byteOrder
              ? buffer.readDoubleLE(offset)
              : buffer.readDoubleBE(offset);
            offset += 8;
            result.push({ x: x, y: y });
          }
          break;
        case 3: // WKBPolygon
          // eslint-disable-next-line no-case-declarations
          const numRings = byteOrder
            ? buffer.readUInt32LE(offset)
            : buffer.readUInt32BE(offset);
          offset += 4;
          result = [];
          for (i = numRings; i > 0; i--) {
            numPoints = byteOrder
              ? buffer.readUInt32LE(offset)
              : buffer.readUInt32BE(offset);
            offset += 4;
            line = [];
            for (j = numPoints; j > 0; j--) {
              x = byteOrder
                ? buffer.readDoubleLE(offset)
                : buffer.readDoubleBE(offset);
              offset += 8;
              y = byteOrder
                ? buffer.readDoubleLE(offset)
                : buffer.readDoubleBE(offset);
              offset += 8;
              line.push({ x: x, y: y });
            }
            result.push(line);
          }
          break;
        case 4: // WKBMultiPoint
        case 5: // WKBMultiLineString
        case 6: // WKBMultiPolygon
        case 7: // WKBGeometryCollection
          // eslint-disable-next-line no-case-declarations
          const num = byteOrder
            ? buffer.readUInt32LE(offset)
            : buffer.readUInt32BE(offset);
          offset += 4;
          result = [];
          for (i = num; i > 0; i--) {
            result.push(parseGeometry());
          }
          break;
      }
      return result;
    }
    return parseGeometry();
  },

  // Wraps JSON.parse to allow use as a defaultCast in text_parser
  JSON(buffer, offset, length) {
    if (typeof offset !== 'undefined' || typeof length !== 'undefined') {
      buffer = buffer.slice(offset, length);
    }
    return JSON.parse(buffer);
  },

  alwaysNull() {
    return null;
  }
}; // End of PacketParser

module.exports = PacketParser;
