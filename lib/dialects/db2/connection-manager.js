'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const ResourceLock = require('./resource-lock');
const Promise = require('../../promise');
const logger = require('../../utils/logger');
const sequelizeErrors = require('../../errors');
const DataTypes = require('../../data-types').db2;
const parserStore = require('../parserStore')('db2');
const debug = logger.getLogger().debugContext('connection:db2');
const debugIbm_db = logger.getLogger().debugContext('connection:db2:ibm_db');

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);

    this.sequelize.config.port = this.sequelize.config.port || 1433;

    try {
      if (sequelize.config.dialectModulePath) {
        this.lib = require(sequelize.config.dialectModulePath);
      } else {
        this.lib = require('ibm_db');
      }
    } catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        throw new Error('Please install ibm_db package manually');
      }
      throw err;
    }

    this.refreshTypeParser(DataTypes);
  }

  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  connect(config) {
    const connectionConfig = {
      database: config.database,
      hostname: config.host,
      port: config.port,
      uid: config.username,
      pwd: config.password
    };

    if (config.ssl) {
         connectionConfig["security"] = config.ssl;
    }
    if (config.sslcertificate) {
        connectionConfig["SSLServerCertificate"] = config.sslcertificate;
    }
    if (config.dialectOptions) {
      for (const key of Object.keys(config.dialectOptions)) {
        connectionConfig[key] = config.dialectOptions[key];
      }
    }

    return new Promise((resolve, reject) => {
      const connection = new this.lib.Database();
      connection.lib = this.lib;

      if (config.dialectOptions && config.dialectOptions.debug) {
        this.lib.debug(true);
      }

      connection.open(connectionConfig, (err) => {
        if (!err) {
          debug('connection acquired');
          const resourceLock = new ResourceLock(connection);
          return resolve(resourceLock);
        }

        if (err) {
          let connError;
          if(err.message && err.message.search("SQL30081N") != -1) {
            connError = new sequelizeErrors.ConnectionRefusedError(err);
          } else {
            connError = new sequelizeErrors.ConnectionError(err);
          }
          connError["message"] = err.message;
          return reject(connError);
        }
      });
    });
  }

  disconnect(connectionLock) {
    /**
     * Abstract connection may try to disconnect raw connection used for fetching version
     */
    const connection = connectionLock.unwrap
      ? connectionLock.unwrap()
      : connectionLock;

    // Don't disconnect a connection that is already disconnected
    if (connection.connected) {
      connection.closeSync();
      debug('connection closed');
    }
    return Promise.resolve();
  }

  validate(connectionLock) {
    /**
     * Abstract connection may try to validate raw connection used for fetching version
     */
    const connection = connectionLock.unwrap
      ? connectionLock.unwrap()
      : connectionLock;

    return connection && connection.loggedIn;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
