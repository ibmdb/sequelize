'use strict';

const _ = require('lodash');
const AbstractDialect = require('../abstract');
const ConnectionManager = require('./connection-manager');
const Query = require('./query');
const QueryGenerator = require('./query-generator');
const DataTypes = require('../../data-types').db2;

class Db2Dialect extends AbstractDialect {
  constructor(sequelize) {
    super();
    this.sequelize = sequelize;
    this.connectionManager = new ConnectionManager(this, sequelize);
    this.QueryGenerator = new QueryGenerator({
      _dialect: this,
      sequelize
    });
  }
}

Db2Dialect.prototype.supports = _.merge(_.cloneDeep(AbstractDialect.prototype.supports), {
  'DEFAULT': true,
  'DEFAULT VALUES': true,
  'LIMIT ON UPDATE': true,
  'ORDER NULLS': false,
  lock: false,
  transactions: true,
  migrations: false,
  returnValues: false, 
  schemas: true,
  autoIncrement: {
    identityInsert: true,
    defaultValue: false,
    update: false
  },
  constraints: {
    restrict: false,
    default: true
  },
  index: {
    collate: false,
    length: false,
    parser: false,
    type: true,
    using: false,
    where: true
  },
  NUMERIC: true,
  tmpTableTrigger: true
});

ConnectionManager.prototype.defaultVersion = '5.0.0'; // Db2 supported version comes here
Db2Dialect.prototype.Query = Query;
Db2Dialect.prototype.name = 'db2';
Db2Dialect.prototype.TICK_CHAR = '"';
Db2Dialect.prototype.TICK_CHAR_LEFT = '';
Db2Dialect.prototype.TICK_CHAR_RIGHT = '';
Db2Dialect.prototype.DataTypes = DataTypes;

module.exports = Db2Dialect;
