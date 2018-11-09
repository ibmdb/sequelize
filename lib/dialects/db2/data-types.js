'use strict';

const _ = require('lodash');
const moment = require('moment');
const inherits = require('../../utils/inherits');

module.exports = BaseTypes => {
  const warn = BaseTypes.ABSTRACT.warn.bind(undefined, 'https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.sql.ref.doc/doc/r0008478.html');

  /**
   * types: [hex, ...]
   * @see Data types and table columns: https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.admin.dbobj.doc/doc/c0055357.html 
   */

  BaseTypes.DATE.types.db2 = ['DATE'];
  BaseTypes.STRING.types.db2 = ['VARCHAR'];
  BaseTypes.CHAR.types.db2 = ['CHAR'];
  BaseTypes.TEXT.types.db2 = ['VARCHAR'];
  BaseTypes.TINYINT.types.db2 = ['SMALLINT'];
  BaseTypes.SMALLINT.types.db2 = ['SMALLINT'];
  BaseTypes.MEDIUMINT.types.db2 = ['INTEGER'];
  BaseTypes.INTEGER.types.db2 = ['INTEGER'];
  BaseTypes.BIGINT.types.db2 = ['BIGINT'];
  BaseTypes.FLOAT.types.db2 = ['REAL'];
  BaseTypes.TIME.types.db2 = ['TIME'];
  BaseTypes.DATEONLY.types.db2 = ['DATE'];
  BaseTypes.BOOLEAN.types.db2 = ['BINARY'];
  BaseTypes.BLOB.types.db2 = ['BLOB'];
  BaseTypes.DECIMAL.types.db2 = ['DECIMAL'];
  BaseTypes.UUID.types.db2 = false;
  BaseTypes.ENUM.types.db2 = false;
  BaseTypes.REAL.types.db2 = ['REAL'];
  BaseTypes.DOUBLE.types.db2 = ['DOUBLE'];
  BaseTypes.GEOMETRY.types.db2 = false;

  function BLOB(length) {
    if (!(this instanceof BLOB)) return new BLOB(length);
    BaseTypes.BLOB.apply(this, arguments);
  }
  inherits(BLOB, BaseTypes.BLOB);

  BLOB.prototype.toSql = function toSql() {
    if(this._length) {
      if (this._length.toLowerCase() === 'tiny') { // tiny = 255 bytes
        return 'BLOB(255)';
      }
      else if (this._length.toLowerCase() === 'medium') { // medium = 16M bytes
        return 'BLOB(16M)';
      }
      else if (this._length.toLowerCase() === 'long') { // long = 2GB
        return 'BLOB(2G)';
      }
      else {
        return 'BLOB(' + this._length + ')';
      }
    } else { return 'BLOB'; } // 1MB
  };

  BLOB.prototype._hexify = function _hexify(hex) {
    return '0x' + hex;
  };

  function STRING(length, binary) {
    if (!(this instanceof STRING)) return new STRING(length, binary);
    BaseTypes.STRING.apply(this, arguments);
  }
  inherits(STRING, BaseTypes.STRING);

  STRING.prototype.toSql = function toSql() {
    if (!this._binary) {
      return 'VARCHAR(' + this._length + ')';
    } else {
      return 'BINARY(' + this._length + ')';
    }
  };

  STRING.prototype.escape = false;
  STRING.prototype._stringify = function _stringify(value, options) {
    if (this._binary) {
      return BLOB.prototype._stringify(value);
    } else {
      return options.escape(value);
    }
  };
  STRING.prototype._bindParam = function _bindParam(value, options) {
    return options.bindParam(this._binary ? Buffer.from(value) : value);
  };

  function TEXT(length) {
    if (!(this instanceof TEXT)) return new TEXT(length);
    BaseTypes.TEXT.apply(this, arguments);
  }
  inherits(TEXT, BaseTypes.TEXT);

  TEXT.prototype.toSql = function toSql() {
    // TEXT is deprecated in db2 and it would normally be saved as a non-unicode string.
    // Using unicode is just future proof
    if (this._length) {
      if (this._length.toLowerCase() === 'tiny') { // tiny = 2^8
        warn('Db2 does not support TEXT with the `length` = `tiny` option. `VARCHAR(256)` will be used instead.');
        return 'VARCHAR(256)';
      }
      warn('Db2 does not support TEXT with the `length` option. `VARCHAR(MAX)` will be used instead.');
    }
    return 'VARCHAR(MAX)';
  };

  function BOOLEAN() {
    if (!(this instanceof BOOLEAN)) return new BOOLEAN();
    BaseTypes.BOOLEAN.apply(this, arguments);
  }
  inherits(BOOLEAN, BaseTypes.BOOLEAN);

  BOOLEAN.prototype.toSql = function toSql() {
    return 'BIT';
  };

  function UUID() {
    if (!(this instanceof UUID)) return new UUID();
    BaseTypes.UUID.apply(this, arguments);
  }
  inherits(UUID, BaseTypes.UUID);

  UUID.prototype.toSql = function toSql() {
    return 'CHAR(36)';
  };

  function NOW() {
    if (!(this instanceof NOW)) return new NOW();
    BaseTypes.NOW.apply(this, arguments);
  }
  inherits(NOW, BaseTypes.NOW);

  NOW.prototype.toSql = function toSql() {
    return 'GETDATE()';
  };

  function DATE(length) {
    if (!(this instanceof DATE)) return new DATE(length);
    BaseTypes.DATE.apply(this, arguments);
  }
  inherits(DATE, BaseTypes.DATE);

  DATE.prototype.toSql = function toSql() {
    return 'TIMESTAMP';
  };

  function DATEONLY() {
    if (!(this instanceof DATEONLY)) return new DATEONLY();
    BaseTypes.DATEONLY.apply(this, arguments);
  }
  inherits(DATEONLY, BaseTypes.DATEONLY);

  DATEONLY.parse = function(value) {
    return moment(value).format('YYYY-MM-DD');
  };

  function INTEGER(length) {
    if (!(this instanceof INTEGER)) return new INTEGER(length);
    BaseTypes.INTEGER.apply(this, arguments);

    // Db2 does not support any options for integer
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('Db2 does not support INTEGER with options. Plain INTEGER will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(INTEGER, BaseTypes.INTEGER);

  function TINYINT(length) {
    if (!(this instanceof TINYINT)) return new TINYINT(length);
    BaseTypes.TINYINT.apply(this, arguments);

    // Db2 does not support any options for tinyint
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('Db2 does not support TINYINT with options. Plain TINYINT will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(TINYINT, BaseTypes.TINYINT);

  function SMALLINT(length) {
    if (!(this instanceof SMALLINT)) return new SMALLINT(length);
    BaseTypes.SMALLINT.apply(this, arguments);

    // Db2 does not support any options for smallint
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('Db2 does not support SMALLINT with options. Plain SMALLINT will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(SMALLINT, BaseTypes.SMALLINT);

  function BIGINT(length) {
    if (!(this instanceof BIGINT)) return new BIGINT(length);
    BaseTypes.BIGINT.apply(this, arguments);

    // Db2 does not support any options for bigint
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('Db2 does not support BIGINT with options. Plain `BIGINT` will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(BIGINT, BaseTypes.BIGINT);

  function REAL(length, decimals) {
    if (!(this instanceof REAL)) return new REAL(length, decimals);
    BaseTypes.REAL.apply(this, arguments);

    // Db2 does not support any options for real
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('Db2 does not support REAL with options. Plain `REAL` will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(REAL, BaseTypes.REAL);

  function FLOAT(length, decimals) {
    if (!(this instanceof FLOAT)) return new FLOAT(length, decimals);
    BaseTypes.FLOAT.apply(this, arguments);

    // Db2 does only support lengths as option.
    // Values between 1-24 result in 7 digits precision (4 bytes storage size)
    // Values between 25-53 result in 15 digits precision (8 bytes storage size)
    // If decimals are provided remove these and print a warning
    if (this._decimals) {
      warn('Db2 does not support Float with decimals. Plain `FLOAT` will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
    }
    if (this._unsigned) {
      warn('Db2 does not support Float unsigned. `UNSIGNED` was removed.');
      this._unsigned = undefined;
    }
    if (this._zerofill) {
      warn('Db2 does not support Float zerofill. `ZEROFILL` was removed.');
      this._zerofill = undefined;
    }
  }
  inherits(FLOAT, BaseTypes.FLOAT);

  function ENUM() {
    if (!(this instanceof ENUM)) {
      const obj = Object.create(ENUM.prototype);
      ENUM.apply(obj, arguments);
      return obj;
    }
    BaseTypes.ENUM.apply(this, arguments);
  }
  inherits(ENUM, BaseTypes.ENUM);

  ENUM.prototype.toSql = function toSql() {
    return 'VARCHAR(255)';
  };

 function DOUBLE(length, decimals) {
    if (!(this instanceof DOUBLE)) return new DOUBLE(length, decimals);
    BaseTypes.DOUBLE.apply(this, arguments);

    // db2 does not support any parameters for double
    if (this._length || this.options.length || this._unsigned || this._zerofill) {
      warn('db2 does not support DOUBLE with options. Plain DOUBLE will be used instead.');
      this._length = undefined;
      this.options.length = undefined;
      this._unsigned = undefined;
      this._zerofill = undefined;
    }
  }
  inherits(DOUBLE, BaseTypes.DOUBLE);

  DOUBLE.prototype.toSql = function toSql() {
    return 'DOUBLE';
  }
  DOUBLE.prototype.key = DOUBLE.key = 'DOUBLE';

  const exports = {
    BLOB,
    BOOLEAN,
    ENUM,
    STRING,
    UUID,
    DATE,
    DATEONLY,
    NOW,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    REAL,
    FLOAT,
    TEXT
  };

  _.forIn(exports, (DataType, key) => {
    if (!DataType.key) DataType.key = key;
    if (!DataType.extend) {
      DataType.extend = function extend(oldType) {
        return new DataType(oldType.options);
      };
    }
  });

  return exports;
};
