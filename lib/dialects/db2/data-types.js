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

  BaseTypes.DATE.types.db2 = ['TIMESTAMP'];
  BaseTypes.STRING.types.db2 = ['VARCHAR'];
  BaseTypes.CHAR.types.db2 = ['CHAR'];
  BaseTypes.TEXT.types.db2 = ['VARCHAR', 'CLOB'];
  BaseTypes.TINYINT.types.db2 = ['SMALLINT'];
  BaseTypes.SMALLINT.types.db2 = ['SMALLINT'];
  BaseTypes.MEDIUMINT.types.db2 = ['INTEGER'];
  BaseTypes.INTEGER.types.db2 = ['INTEGER'];
  BaseTypes.BIGINT.types.db2 = ['BIGINT'];
  BaseTypes.FLOAT.types.db2 = ['DOUBLE', 'REAL', 'FLOAT'];
  BaseTypes.TIME.types.db2 = ['TIME'];
  BaseTypes.DATEONLY.types.db2 = ['DATE'];
  BaseTypes.BOOLEAN.types.db2 = ['BOOLEAN', 'SMALLINT', 'BIT'];
  BaseTypes.BLOB.types.db2 = ['BLOB'];
  BaseTypes.DECIMAL.types.db2 = ['DECIMAL'];
  BaseTypes.UUID.types.db2 = ['CHAR () FOR BIT DATA'];
  BaseTypes.ENUM.types.db2 = ['VARCHAR'];
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

  BLOB.prototype.escape = function (blob) {
    return "BLOB('" + blob.toString().replace(/'/g, "''") + "')";
  };

  BLOB.prototype._stringify = function _stringify(value) {
    if (Buffer.isBuffer(value)) {
      return "BLOB('" + value.toString().replace(/'/g, "''") + "')";
    } else {
      if (Array.isArray(value)) {
        value = Buffer.from(value);
      } else {
        value = Buffer.from(value.toString());
      }
      const hex = value.toString('hex');
      return this._hexify(hex);
    }
  };

  BLOB.prototype._hexify = function _hexify(hex) {
    return "x'" + hex + "'";
  };

  function STRING(length, binary) {
    if (!(this instanceof STRING)) return new STRING(length, binary);
    BaseTypes.STRING.apply(this, arguments);
  }
  inherits(STRING, BaseTypes.STRING);

  STRING.prototype.toSql = function toSql() {
    if (!this._binary) {
      if (this._length <= 4000) {
        return 'VARCHAR(' + this._length + ')';
      } else {
        return 'CLOB(' + this._length + ')';
      }
    } else {
      if (this._length < 255) {
        return 'CHAR(' + this._length + ') FOR BIT DATA';
      } else if (this._length <= 4000) {
        return 'VARCHAR(' + this._length + ') FOR BIT DATA';
      } else {
        return 'BLOB(' + this._length + ')';
      }
    }
  };

  STRING.prototype.escape = false;
  STRING.prototype._stringify = function _stringify(value, options) {
    if (this._binary) {
      return BLOB.prototype._hexify(value.toString('hex'));
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
    let len = 0;
    if (this._length) {
      switch (this._length.toLowerCase()) {
        case 'tiny':
          len = 256;  // tiny = 2^8
          break;
        case 'medium':
          len = 8192;  // medium = 2^13 = 8k
          break;
        case 'long':
          len = 65536;  // long = 64k
          break;
      }
      if( isNaN(this._length) ) {
          this._length = 32672;
      }
      if(len > 0 ) { this._length = len; }
    } else {  this._length = 32672; }
    if( this._length > 32672 )
    {
      len = 'CLOB(' + this._length + ')';
    }
    else
    {
      len = 'VARCHAR(' + this._length + ')';
    }
    warn('Db2 does not support TEXT datatype. '+len+' will be used instead.');
    return len;
  };

  function BOOLEAN() {
    if (!(this instanceof BOOLEAN)) return new BOOLEAN();
    BaseTypes.BOOLEAN.apply(this, arguments);
  }
  inherits(BOOLEAN, BaseTypes.BOOLEAN);

  BOOLEAN.prototype.toSql = function toSql() {
    return 'BOOLEAN';
  };

  BOOLEAN.prototype._sanitize = function _sanitize(value) {
    if (value !== null && value !== undefined) {
      if (Buffer.isBuffer(value) && value.length === 1) {
        // Bit fields are returned as buffers
        value = value[0];
      }

      if (_.isString(value)) {
        // Only take action on valid boolean strings.
        value = value === 'true' ? true : value === 'false' ? false : value;
        value = value === '\u0001' ? true : value === '\u0000' ? false : value;

      } else if (_.isNumber(value)) {
        // Only take action on valid boolean integers.
        value = value === 1 ? true : value === 0 ? false : value;
      }
    }

    return value;
  };
  BOOLEAN.parse = BOOLEAN.prototype._sanitize;

  function UUID() {
    if (!(this instanceof UUID)) return new UUID();
    BaseTypes.UUID.apply(this, arguments);
  }
  inherits(UUID, BaseTypes.UUID);

  UUID.prototype.toSql = function toSql() {
    return 'CHAR(36) FOR BIT DATA';
  };

  function NOW() {
    if (!(this instanceof NOW)) return new NOW();
    BaseTypes.NOW.apply(this, arguments);
  }
  inherits(NOW, BaseTypes.NOW);

  NOW.prototype.toSql = function toSql() {
    return 'CURRENT TIME';
  };

  function DATE(length) {
    if (!(this instanceof DATE)) return new DATE(length);
    BaseTypes.DATE.apply(this, arguments);
  }
  inherits(DATE, BaseTypes.DATE);

  DATE.prototype.toSql = function toSql() {
    if(this._length < 0) { this._length = 0; }
    if(this._length > 6) { this._length = 6; }
    return 'TIMESTAMP' + (this._length ? '(' + this._length + ')' : '');
  };

  DATE.prototype._stringify = function _stringify(date, options) {
    date = BaseTypes.DATE.prototype._applyTimezone(date, options);
    if (this._length > 0) {
      let msec = '.';
      for( let i = 0; i < this._length && i < 6; i++ ) {
        msec += 'S';
      }
      return date.format('YYYY-MM-DD HH:mm:ss' + msec);
    }

    return date.format('YYYY-MM-DD HH:mm:ss');
  };

  DATE.prototype.parse = function parse(value, options) {
    value = value.string();

    if (value === null) {
      return value;
    }

    if (moment.tz.zone(options.timezone)) {
      value = moment.tz(value, options.timezone).toDate();
    } else {
      value = new Date(value + ' ' + options.timezone);
    }

    return value;
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
    if (this._length || this.options.length || this._unsigned || this._zerofill)
    {
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
    DOUBLE,
    'DOUBLE PRECISION': DOUBLE,
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
