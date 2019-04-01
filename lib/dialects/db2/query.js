'use strict';

const logger = require('../../utils/logger');
const Promise = require('../../promise');
const AbstractQuery = require('../abstract/query');
const sequelizeErrors = require('../../errors');
const parserStore = require('../parserStore')('db2');
const _ = require('lodash');
const debug = logger.getLogger().debugContext('sql:db2');

class Query extends AbstractQuery {
  constructor(connection, sequelize, options) {
    super();
    this.connection = connection;
    this.instance = options.instance;
    this.model = options.model;
    this.sequelize = sequelize;
    this.options = _.extend({
      logging: console.log,
      plain: false,
      raw: false
    }, options || {});

    this.checkLoggingOption();
  }

  getInsertIdField() {
    return 'id';
  }

  getSQLTypeFromJsType(value) {
    const param = {ParamType:"INPUT", Data: value};
    if (Buffer.isBuffer(value)) {
      param.DataType = "BLOB";
      return param;
    }
    return value;
  }

  _run(connection, sql, parameters) {
    this.sql = sql;

    //do we need benchmark for this query execution
    const benchmark = this.sequelize.options.benchmark || this.options.benchmark;
    let queryBegin;
    if (benchmark) {
      queryBegin = Date.now();
    } else {
      this.sequelize.log('Executing (' + (this.connection.uuid || 'default') + '): ' + this.sql, this.options);
    }

    debug(`executing(${this.connection.uuid || 'default'}) : ${this.sql}`);

    return new Promise((resolve, reject) => {
      // TRANSACTION SUPPORT
      if (_.startsWith(this.sql, 'BEGIN TRANSACTION')) {
        connection.beginTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        });
      } else if (_.startsWith(this.sql, 'COMMIT TRANSACTION')) {
        connection.commitTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        });
      } else if (_.startsWith(this.sql, 'ROLLBACK TRANSACTION')) {
        connection.rollbackTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        });
      } else if (_.startsWith(this.sql, 'SAVE TRANSACTION')) {
        connection.commitTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        }, this.options.transaction.name);
      } else {
        const results = [];
        let params = [];
        if (parameters) {
          _.forOwn(parameters, (value, key) => {
            const param = this.getSQLTypeFromJsType(value);
            params.push(param);
          });
        }

        connection.prepare(this.sql, (err, stmt) => {
          if(err) { reject(this.formatError(err)); }
          stmt.execute(params, (err, result, outparams) => {
            debug(`executed(${this.connection.uuid || 'default'}):${this.sql}`);

            if (benchmark) {
              this.sequelize.log('Executed (' + (this.connection.uuid || 'default') + '): ' + this.sql, Date.now() - queryBegin, this.options);
            }

            if (err && err.message) {
              if(err.message.search("SQL0204N") != -1) {
                err = null; // Ignore table not found error for drop table.
              } else if(err.message.search("SQL0478N") != -1) {
                err = null; // Ignore drop schema error.
              } else if(err.message.search("SQL0601N") != -1) {
                err = null; // Ignore create schema error.
              } else if(err.message.search("SQL0605W") != -1) {
                //warn(err.message);
                err = null; // Ignore warning.
              }
            }
            if (err) {
              err.sql = sql;
              reject(this.formatError(err));
            } else {
              let data = [];
              let metadata = [];
              if(typeof(result) === 'object') {
                data = result.fetchAllSync();
                metadata = result.getColumnMetadataSync();
                result.closeSync();
              }
              const datalen = data.length;
              if(datalen > 0) {
                let coltypes = {};
                for (let i = 0; i < metadata.length; i++) {
                  coltypes[metadata[i].SQL_DESC_CONCISE_TYPE] =
                      metadata[i].SQL_DESC_TYPE_NAME;
                }
                for(let i = 0; i < datalen; i++) {
                  for(const column in data[i]) {
                    const parse = parserStore.get(coltypes[column]);
                    let value = data[i][column];
                    if (value !== null & !!parse) {
                      value = parse(value);
                      data[i][column] = value;
                    }
                  }
                }
                resolve(this.formatResults(data, datalen, metadata));
              } else {
                resolve(this.formatResults(data, 0));
              }
            }
          });
        });
      }
    });
  }

  run(sql, parameters) {
    return Promise.using(this.connection.lock(), connection => this._run(connection, sql, parameters));
  }

  static formatBindParameters(sql, values, dialect) {
    const bindParam = {};
    const replacementFunc = (match, key, values) => {
      if (values[key] !== undefined) {
        bindParam[key] = values[key];
        return '?';
      }
      return undefined;
    };
    sql = AbstractQuery.formatBindParameters(sql, values, dialect, replacementFunc)[0];

    return [sql, bindParam];
  }

  /**
   * High level function that handles the results of a query execution.
   *
   *
   * Example:
   *  query.formatResults([
   *    {
   *      id: 1,              // this is from the main table
   *      attr2: 'snafu',     // this is from the main table
   *      Tasks.id: 1,        // this is from the associated table
   *      Tasks.title: 'task' // this is from the associated table
   *    }
   *  ])
   *
   * @param {Array} data - The result of the query execution.
   * @private
   */
  formatResults(data, rowCount, metadata) {
    let result = this.instance;
    if (this.isInsertQuery(data, metadata)) {
      this.handleInsertQuery(data, metadata);

      if (!this.instance) {
        if (this.options.plain) {
          const record = data[0];
          result = record[Object.keys(record)[0]];
        } else {
          result = data;
        }
      }
    }

    if (this.isShowTablesQuery()) {
      result = this.handleShowTablesQuery(data);
    } else if (this.isDescribeQuery()) {
      result = {};
      for (const _result of data) {
        if (_result.Default) {
          _result.Default = _result.Default.replace("('", '').replace("')", '').replace(/'/g, '');
        }

        result[_result.Name] = {
          type: _result.Type.toUpperCase(),
          allowNull: _result.IsNull === 'YES' ? true : false,
          defaultValue: _result.Default,
          primaryKey: _result.Constraint === 'PRIMARY KEY',
          autoIncrement: _result.IsIdentity === 1,
          comment: _result.Comment
        };
      }
    } else if (this.isShowIndexesQuery()) {
      result = this.handleShowIndexesQuery(data);
    } else if (this.isSelectQuery()) {
      result = this.handleSelectQuery(data);
    } else if (this.isUpsertQuery()) {
      result = data[0];
    } else if (this.isCallQuery()) {
      result = data[0];
    } else if (this.isBulkUpdateQuery()) {
      result = data.length;
    } else if (this.isBulkDeleteQuery()) {
      result = data[0] && data[0].AFFECTEDROWS;
    } else if (this.isVersionQuery()) {
      result = data;
    } else if (this.isForeignKeysQuery()) {
      result = data;
    } else if (this.isInsertQuery() || this.isUpdateQuery()) {
      result = [result, rowCount];
    } else if (this.isShowConstraintsQuery()) {
      result = this.handleShowConstraintsQuery(data);
    } else if (this.isRawQuery()) {
      // Db2 returns row data and metadata (affected rows etc) in a single object - let's standarize it, sorta
      result = [data, data];
    }

    return result;
  }

  handleShowTablesQuery(results) {
    return results.map(resultSet => {
      return {
        tableName: resultSet.TABLE_NAME,
        schema: resultSet.TABLE_SCHEMA
      };
    });
  }

  handleShowConstraintsQuery(data) {
    //Convert snake_case keys to camelCase as it's generated by stored procedure
    return data.slice(1).map(result => {
      const constraint = {};
      for (const key in result) {
        constraint[_.camelCase(key)] = result[key];
      }
      return constraint;
    });
  }

  formatError(err) {
    let match;

    match = err.message.match(/SQL0803N/);
    if (match && match.length > 0) {
      let fields = {};
      const uniqueKey = this.model && this.model.uniqueKeys[match[1]];
      let message = err.message;

      if (uniqueKey && !!uniqueKey.msg) {
        message = uniqueKey.msg;
      }
      if (match[4]) {
        const values = match[4].split(',').map(part => part.trim());
        if (uniqueKey) {
          fields = _.zipObject(uniqueKey.fields, values);
        } else {
          fields[match[1]] = match[4];
        }
      }

      const errors = [];
      _.forOwn(fields, (value, field) => {
        errors.push(new sequelizeErrors.ValidationErrorItem(
          this.getUniqueConstraintErrorMessage(field),
          'unique violation', // sequelizeErrors.ValidationErrorItem.Origins.DB,
          field,
          value,
          this.instance,
          'not_unique'
        ));
      });

      return new sequelizeErrors.UniqueConstraintError({ message, errors, parent: err, fields });
    }

    match = err.message.match(/SQL0532N  A parent row cannot be deleted because the relationship "(.*)" restricts the deletion/) ||
      err.message.match(/SQL0530N/) ||
      err.message.match(/SQL0531N/) ;
    if (match && match.length > 0) {
      return new sequelizeErrors.ForeignKeyConstraintError({
        fields: null,
        index: match[1],
        parent: err
      });
    }

    match = err.message.match(/Could not drop constraint. See previous errors./);
    if (match && match.length > 0) {
      let constraint = err.sql.match(/(?:constraint|index) \[(.+?)\]/i);
      constraint = constraint ? constraint[1] : undefined;
      let table = err.sql.match(/table \[(.+?)\]/i);
      table = table ? table[1] : undefined;

      return new sequelizeErrors.UnknownConstraintError({
        message: match[1],
        constraint,
        table,
        parent: err
      });
    }

    return new sequelizeErrors.DatabaseError(err);
  }

  isShowOrDescribeQuery() {
    let result = false;

    result = result || this.sql.toLowerCase().indexOf("select c.column_name as 'name', c.data_type as 'type', c.is_nullable as 'isnull'") === 0;
    result = result || this.sql.toLowerCase().indexOf('select tablename = t.name, name = ind.name,') === 0;
    result = result || this.sql.toLowerCase().indexOf('exec sys.sp_helpindex @objname') === 0;

    return result;
  }

  isShowIndexesQuery() {
    return this.sql.toLowerCase().indexOf('exec sys.sp_helpindex @objname') === 0;
  }

  handleShowIndexesQuery(data) {
    // Group by index name, and collect all fields
    data = _.reduce(data, (acc, item) => {
      if (!(item.index_name in acc)) {
        acc[item.index_name] = item;
        item.fields = [];
      }

      _.forEach(item.index_keys.split(','), column => {
        let columnName = column.trim();
        if (columnName.indexOf('(-)') !== -1) {
          columnName = columnName.replace('(-)', '');
        }

        acc[item.index_name].fields.push({
          attribute: columnName,
          length: undefined,
          order: column.indexOf('(-)') !== -1 ? 'DESC' : 'ASC',
          collate: undefined
        });
      });
      delete item.index_keys;
      return acc;
    }, {});

    return _.map(data, item => ({
      primary: item.index_name.toLowerCase().indexOf('pk') === 0,
      fields: item.fields,
      name: item.index_name,
      tableName: undefined,
      unique: item.index_description.toLowerCase().indexOf('unique') !== -1,
      type: undefined
    }));
  }

  handleInsertQuery(results, metaData) {
    if (this.instance) {
      // add the inserted row id to the instance
      const autoIncrementAttribute = this.model.autoIncrementAttribute;
      let id = null;
      let autoIncrementAttributeAlias = null;

      if (this.model.rawAttributes.hasOwnProperty(autoIncrementAttribute) &&
        this.model.rawAttributes[autoIncrementAttribute].field !== undefined)
        autoIncrementAttributeAlias = this.model.rawAttributes[autoIncrementAttribute].field;

      id = id || results &&  results[0] && results[0][this.getInsertIdField()];
      id = id || metaData && metaData[this.getInsertIdField()];
      id = id || results &&  results[0] && results[0][autoIncrementAttribute];
      id = id || autoIncrementAttributeAlias && results && results[0] && results[0][autoIncrementAttributeAlias];

      this.instance[autoIncrementAttribute] = id;
    }
  }
}

module.exports = Query;
module.exports.Query = Query;
module.exports.default = Query;
