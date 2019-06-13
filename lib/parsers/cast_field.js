'use strict';

const PacketParser = require('./packet_parser');

// node-mysql typeCast compatibility wrapper
// see https://github.com/mysqljs/mysql/blob/96fdd0566b654436624e2375c7b6604b1f50f825/lib/protocol/packets/Field.js
class CastField {
  constructor(isBinary, column, encoding, defaultCast, fieldBuffer) {
    this.isBinary = isBinary;
    this.column = column;

    if (fieldBuffer === undefined || fieldBuffer === null) {
      fieldBuffer = Buffer.alloc(0);
      encoding = null;

      this.defaultRead = PacketParser.alwaysNull;
    } else if (typeof defaultCast === 'function') {
      this.defaultRead = function cast() {
        return defaultCast(fieldBuffer);
      };
    } else if (
      encoding === 'binary' ||
      encoding === null ||
      encoding === undefined
    ) {
      this.defaultRead = function buffer() {
        return fieldBuffer;
      };
    } else {
      this.defaultRead = function decode() {
        return PacketParser.decode(fieldBuffer, encoding);
      };
    }

    this.fieldBuffer = fieldBuffer;

    this.forceEncoding = encoding;
  }

  run(typeCast) {
    return typeCast(this, this.defaultRead);
  }

  toString(encoding) {
    return PacketParser.decode(
      this.fieldBuffer,
      arguments.length ? encoding : this.encoding
    );
  }

  get encoding() {
    return this.forceEncoding || this.column.encoding;
  }

  // node-mysql typeCast API: Packet access methods
  string() {
    if (this.isBinary) {
      // When providing a typeCast interface to a binary packet,
      // defaultRead().toString() is used to provide a text conversion
      const value = this.defaultRead();
      return value === null || value === undefined ? null : value.toString();
    }
    return this.toString();
  }
  buffer() {
    return this.fieldBuffer;
  }
  geometry() {
    return PacketParser.geometry(this.fieldBuffer);
  }

  // node-mysql typeCast API fields
  get db() {
    return this.column.db;
  }
  get table() {
    return this.column.table;
  }
  get name() {
    return this.column.name;
  }
  get type() {
    return this.column.type;
  }
  get length() {
    return this.column.length;
  }

  // node-mysql2 ColumnDefinition API fields
  get columnType() {
    return this.column.columnType;
  }
  get columnLength() {
    return this.column.columnLength;
  }
  get schema() {
    return this.column.schema;
  }
  get orgTable() {
    return this.column.orgTable;
  }
  get orgName() {
    return this.column.orgName;
  }
  get characterSet() {
    return this.column.characterSet;
  }
  get flags() {
    return this.column.flags;
  }
  get decimals() {
    return this.column.decimals;
  }
}

module.exports = CastField;
