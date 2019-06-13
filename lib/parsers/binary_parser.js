'use strict';

const genFunc = require('generate-function');
const FieldFlags = require('../constants/field_flags.js');
const Types = require('../constants/types.js');
const helpers = require('../helpers');
const CastField = require('./cast_field');
const PacketParser = require('./packet_parser');
const parserCache = require('./parser_cache.js');

/**
 * Generates code to
 * • read the raw buffer for a binary field, and
 * • get the default decoder encoding for the buffer
 * • perform the default cast on the Buffer (or null for Buffer / String pass-through)
 *
 * @param {ColumnDefinition} field
 * @param {number} fieldNum
 * @param {object} options
 * @param {object} config
 * @returns {{buffer:String, encoding:String, defaultCast:String}}
 */
function readCodeFor(field, fieldNum, options, config) {
  const supportBigNumbers =
    options.supportBigNumbers || config.supportBigNumbers;
  const bigNumberStrings = options.bigNumberStrings || config.bigNumberStrings;
  const unsigned = field.flags & FieldFlags.UNSIGNED;

  let buffer, encoding, defaultCast;
  switch (field.columnType) {
    case Types.TINY:
      buffer = 'packet.readBuffer(1)';
      encoding = '"binary"';
      defaultCast = unsigned ? 'PacketParser.uint8' : 'PacketParser.int8';
      break;

    case Types.SHORT:
      buffer = 'packet.readBuffer(2)';
      encoding = '"binary"';
      defaultCast = unsigned ? 'PacketParser.uint16' : 'PacketParser.int16';
      break;

    case Types.LONG:
    case Types.INT24: // in binary protocol int24 is encoded in 4 bytes int32
      buffer = 'packet.readBuffer(4)';
      encoding = '"binary"';
      defaultCast = unsigned ? 'PacketParser.uint32' : 'PacketParser.int32';
      break;

    case Types.YEAR:
      buffer = 'packet.readBuffer(2)';
      encoding = '"binary"';
      defaultCast = 'PacketParser.uint16';
      break;

    case Types.FLOAT:
      buffer = 'packet.readBuffer(4)';
      encoding = '"binary"';
      defaultCast = 'PacketParser.float32';
      break;

    case Types.DOUBLE:
      buffer = 'packet.readBuffer(8)';
      encoding = '"binary"';
      defaultCast = 'PacketParser.float64';
      break;

    case Types.NULL:
      buffer = 'null';
      encoding = '"binary"';
      defaultCast = 'PacketParser.alwaysNull';
      break;

    case Types.DATE:
    case Types.DATETIME:
    case Types.TIMESTAMP:
    case Types.NEWDATE:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = '"binary"';
      if (config.dateStrings) {
        defaultCast = !field.decimals
          ? 'PacketParser.dateTimeString'
          : `function(_) { return PacketParser.dateTimeString(_, 0, ${field.decimals |
              0}) }`;
      } else {
        defaultCast = 'PacketParser.dateTime';
      }
      break;

    case Types.TIME:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = '"binary"';
      defaultCast = 'PacketParser.timeString';
      break;

    case Types.DECIMAL:
    case Types.NEWDECIMAL:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = '"ascii"';
      if (config.decimalNumbers) {
        defaultCast = 'PacketParser.floatAscii';
      } else {
        defaultCast = 'null'; // no casting required (other than String conversion)
      }
      break;

    case Types.GEOMETRY:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = '"binary"';
      defaultCast = 'PacketParser.geometry';
      break;

    case Types.JSON:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = '"utf8"';
      defaultCast = 'JSON.parse'; // The only conversion required for UTF-8 buffer
      break;

    case Types.LONGLONG:
      buffer = 'packet.readBuffer(8)';
      encoding = '"binary"';
      if (!supportBigNumbers) {
        defaultCast = unsigned
          ? 'PacketParser.uint64Number'
          : 'PacketParser.int64Number';
      } else if (bigNumberStrings) {
        defaultCast = unsigned
          ? 'PacketParser.uint64String'
          : 'PacketParser.int64String';
      } else {
        defaultCast = unsigned
          ? 'PacketParser.uint64NumberIfPossible'
          : 'PacketParser.int64NumberIfPossible';
      }
      break;

    default:
      buffer = 'packet.readLengthCodedBuffer()';
      encoding = `fields[${fieldNum}].encoding`;
      defaultCast = 'null'; // no casting required (other than String conversion)
      break;
  }
  return { buffer, encoding, defaultCast };
}

function binaryField(column, encoding, typeCast, defaultCast, fieldBuffer) {
  return new CastField(true, column, encoding, defaultCast, fieldBuffer).run(
    typeCast
  );
}

function compile(fields, options, config) {
  const parserFn = genFunc();
  let i = 0;
  const nullBitmapLength = Math.floor((fields.length + 7 + 2) / 8);

  parserFn('(function(){')(
    'return function BinaryRow(packet, fields, options) {'
  );

  if (options.rowsAsArray) {
    parserFn(`const result = new Array(${fields.length});`);
  }

  if (
    typeof options.binaryCast === 'undefined' &&
    typeof config.binaryCast !== 'undefined'
  ) {
    options.binaryCast = config.binaryCast;
  }
  if (
    typeof options.typeCast !== 'function' &&
    typeof config.typeCast === 'function'
  ) {
    options.typeCast = config.typeCast;
  }

  const cast =
    typeof options.binaryCast === 'function'
      ? options.binaryCast
      : options.binaryCast === true && typeof options.typeCast === 'function'
        ? options.typeCast
        : null;

  const parserImports = { cast, binaryField, PacketParser };

  const resultTables = {};
  let resultTablesArray = [];

  if (options.nestTables === true) {
    for (i = 0; i < fields.length; i++) {
      resultTables[fields[i].table] = 1;
    }
    resultTablesArray = Object.keys(resultTables);
    for (i = 0; i < resultTablesArray.length; i++) {
      parserFn(`this[${helpers.srcEscape(resultTablesArray[i])}] = {};`);
    }
  }

  parserFn('packet.readInt8();'); // status byte
  for (i = 0; i < nullBitmapLength; ++i) {
    parserFn(`const nullBitmaskByte${i} = packet.readInt8();`);
  }

  let currentFieldNullBit = 4;
  let nullByteIndex = 0;

  for (i = 0; i < fields.length; i++) {
    const field = fields[i];
    const fieldName = helpers.srcEscape(field.name);
    let lvalue;
    parserFn(`// ${fieldName}: ${field.type}`);

    if (typeof options.nestTables === 'string') {
      lvalue = `this[${helpers.srcEscape(
        field.table + options.nestTables + field.name
      )}]`;
    } else if (options.nestTables === true) {
      lvalue = `this[${helpers.srcEscape(field.table)}][${fieldName}]`;
    } else if (options.rowsAsArray) {
      lvalue = `result[${i}]`;
    } else {
      lvalue = `this[${fieldName}]`;
    }

    const { buffer, encoding, defaultCast } = readCodeFor(
      field,
      i,
      options,
      config
    );

    const isNull = `nullBitmaskByte${nullByteIndex} & ${currentFieldNullBit}`;

    if (cast !== null) {
      parserFn(`${lvalue} = binaryField(fields[${i}], ${encoding}, cast, ${defaultCast}, 
        ${isNull} ? null : ${buffer}
      );`);
    } else {
      parserFn(`${lvalue} = ${isNull} ? null`);

      if (defaultCast !== 'null') {
        parserFn(`: ${defaultCast}(${buffer});`);
      } else if (
        encoding === '"binary"' ||
        encoding === null ||
        encoding === undefined
      ) {
        parserFn(`: ${buffer};`);
      } else {
        parserFn(`: PacketParser.decode(${buffer}, ${encoding});`);
      }
    }

    currentFieldNullBit *= 2;
    if (currentFieldNullBit === 0x100) {
      currentFieldNullBit = 1;
      nullByteIndex++;
    }
  }

  if (options.rowsAsArray) {
    parserFn('return result;');
  }

  parserFn('};')('})()');

  if (config.debug) {
    helpers.printDebugWithCode(
      'Compiled binary protocol row parser',
      parserFn.toString()
    );
  }
  return parserFn.toFunction(parserImports);
}

function getBinaryParser(fields, options, config) {
  return parserCache.getParser('binary', fields, options, config, compile);
}

module.exports = getBinaryParser;
