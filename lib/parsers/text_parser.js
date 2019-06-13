'use strict';

const genFunc = require('generate-function');
const helpers = require('../helpers');
const Types = require('../constants/types.js');
const CastField = require('./cast_field');
const PacketParser = require('./packet_parser');
const parserCache = require('./parser_cache.js');

/**
 * Generates code to
 * • read the raw buffer for a text packet field, and
 * • get the default encoding for the buffer
 * • perform the default cast on the Buffer (or null for Buffer / String pass-through)
 *   Default Cast must be able to accept (buffer) and (buffer, offset, length) as arguments.
 *
 * @param {ColumnDefinition} field
 * @param {number} fieldNum
 * @param {object} options
 * @param {object} config
 * @returns {{buffer:String, encoding:String, defaultCast:String}}
 */
const readCodeFor = function(field, fieldNum, options, config) {
  let encoding;
  let defaultCast;

  switch (field.columnType) {
    case Types.TINY:
    case Types.SHORT:
    case Types.LONG:
    case Types.INT24:
    case Types.YEAR:
      encoding = '"ascii"';
      defaultCast = 'PacketParser.intAsciiSmall'; // parseLengthCodedIntNoBigCheck()
      break;

    case Types.LONGLONG:
      encoding = '"ascii"';
      if (options.supportBigNumbers || config.supportBigNumbers) {
        if (options.bigNumberStrings || config.bigNumberStrings) {
          defaultCast = 'null'; // 'packet.parseLengthCodedIntString()';
        } else {
          defaultCast = 'PacketParser.intAscii'; //`packet.parseLengthCodedInt(true)`;
        }
      } else {
        defaultCast = 'PacketParser.intAsciiSmall'; //`packet.parseLengthCodedInt(false)`;
      }
      break;

    case Types.FLOAT:
    case Types.DOUBLE:
      encoding = '"ascii"';
      defaultCast = 'PacketParser.floatAscii'; // packet.parseLengthCodedFloat()
      break;

    case Types.NULL:
      encoding = '"binary"';
      defaultCast = 'PacketParser.alwaysNull';
      break;

    case Types.DATE:
    case Types.NEWDATE:
      encoding = '"ascii"';
      defaultCast = config.dateStrings ? 'null' : 'PacketParser.dateAscii'; //packet.parseDate
      break;

    case Types.DATETIME:
    case Types.TIMESTAMP:
      encoding = '"ascii"';
      defaultCast = config.dateStrings ? 'null' : 'PacketParser.dateAscii'; //packet.parseDateTime
      break;

    case Types.TIME:
      encoding = '"ascii"';
      defaultCast = 'null';
      break;

    case Types.DECIMAL:
    case Types.NEWDECIMAL:
      encoding = '"ascii"';
      defaultCast = config.decimalNumbers ? 'PacketParser.floatAscii' : 'null';
      break;

    case Types.GEOMETRY:
      encoding = '"binary"';
      defaultCast = 'PacketParser.geometry';
      break;

    case Types.JSON:
      encoding = '"utf8"';
      defaultCast = 'PacketParser.JSON';
      break;

    default:
      encoding = `fields[${fieldNum}].encoding`;
      defaultCast = 'null';
      break;
  }
  return {
    buffer: readCodeFor.buffer,
    encoding,
    defaultCast
  };
};
readCodeFor.buffer = 'packet.readLengthCodedBuffer()';

function castField(column, encoding, typeCast, defaultCast, fieldBuffer) {
  return new CastField(false, column, encoding, defaultCast, fieldBuffer).run(
    typeCast
  );
}

// Accelerated version of castField() for when typeCast === true
function textField(encoding, defaultCast, packet) {
  const length = packet.readLengthCodedNumber();
  if (length === null) {
    return null;
  }
  if (typeof defaultCast === 'function') {
    // defaultCast can often avoid creating intermediate Buffer
    const value = defaultCast(packet.buffer, packet.offset, length);
    packet.offset += length;
    return value;
  }
  const buffer = packet.readBuffer(length);
  if (encoding === 'binary' || encoding === null || encoding === undefined) {
    return buffer;
  }
  return PacketParser.decode(buffer, encoding);
}

function compile(fields, options, config) {
  // use global typeCast if current query doesn't specify one
  if (
    typeof config.typeCast !== 'undefined' &&
    typeof options.typeCast === 'undefined'
  ) {
    options.typeCast = config.typeCast;
  }
  const cast = options.typeCast;

  const parserImports = { cast, castField, textField, PacketParser };

  const parserFn = genFunc();
  let i = 0;

  parserFn('(function() {')(
    'return function TextRow(packet, fields, options) {'
  );

  if (options.rowsAsArray) {
    parserFn(`const result = new Array(${fields.length})`);
  }

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

    let rValue;
    if (cast === false) {
      rValue = readCodeFor.buffer;
    } else {
      const { buffer, encoding, defaultCast } = readCodeFor(
        field,
        i,
        options,
        config
      );

      if (typeof cast === 'function') {
        rValue = `castField(fields[${i}], ${encoding}, cast, ${defaultCast}, ${buffer})`;
      } else {
        rValue = `textField(${encoding}, ${defaultCast}, packet)`;
      }
    }
    parserFn(`${lvalue} = ${rValue};`);
  }

  if (options.rowsAsArray) {
    parserFn('return result;');
  }

  parserFn('};')('})()');

  if (config.debug) {
    helpers.printDebugWithCode(
      'Compiled text protocol row parser',
      parserFn.toString()
    );
  }
  return parserFn.toFunction(parserImports);
}

function getTextParser(fields, options, config) {
  return parserCache.getParser('text', fields, options, config, compile);
}

module.exports = getTextParser;
