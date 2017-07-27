/**
 * Created by xiangyuguo on 17/1/24.
 */
'use strict';

var _ = require('lodash');
var mysql = require('mysql');
var async = require('async');
var Utils = require('./utils.js');
var MysqlConfig = require('./mysql.json');

var DBInfo = function () {
  this.connect = null;
  this.tableNames = null;
  this.createTableSQL = null;
  this.tables = null;
  this.columns = null;
  this.statistics = null;
};

DBInfo.prototype.init = function (dbConfig, cb) {
  var self = this;

  self.connect = mysql.createConnection(dbConfig);

  async.waterfall([
    function (cb) {
      // 取表信息
      self.connect.query("Select * From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA=? And TABLE_TYPE='BASE TABLE'",
        [dbConfig.database], cb);
    },
    function (rows, fields, cb) {
      self.tables = rows;
      self.tableNames = _.map(self.tables, 'TABLE_NAME');

      // 取字段信息
      self.connect.query("Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA=? And TABLE_NAME In ('" +
        self.getTableNames().join("','") + "')", [dbConfig.database], cb);
    },
    function (rows, fields, cb) {
      self.columns = rows;

      // 取主键、索引等其他信息
      self.connect.query("Select * From INFORMATION_SCHEMA.STATISTICS Where TABLE_SCHEMA=? And TABLE_NAME In ('" +
        self.getTableNames().join("','") + "')", [dbConfig.database], cb);
    },
    function (rows, fields, cb) {
      self.statistics = rows;

      // 取所有表结构SQL
      var tasks = [];
      var tables = self.getTableNames();
      _.each(tables, function (tbName) {
        tasks.push(function (cb) {
          self.connect.query("SHOW CREATE TABLE " + tbName, [], cb);
        });
      });

      async.parallelLimit(tasks, 5, function (err, sqls) {
        if (err) {
          cb(err);
          return;
        }

        self.createTableSQL = {};
        _.each(sqls, function (sql, idx) {
          self.createTableSQL[tables[idx]] = sql[0][0]['Create Table'];
        });

        cb(null);
      });
    }
  ], function (err) {
    if (err) {
      if (self.connect) self.connect.end();
    }

    Utils.invokeCallback(cb, err);
  });
};

DBInfo.prototype.getDbName = function () {
  if (!this.connect) {
    return '';
  }

  return this.connect.config.database;
};

DBInfo.prototype.getTableNames = function () {
  return this.tableNames;
};

DBInfo.prototype.getTableColumnNames = function (tableName) {
  return _.map(this.getTableColumnInfo(tableName), 'COLUMN_NAME');
};

DBInfo.prototype.getTableColumnInfo = function (tableName) {
  return _.filter(this.columns, function (column) {
    return column.TABLE_NAME === tableName;
  });
};

DBInfo.prototype.getTableKeyInfo = function (tableName) {
  return _.filter(this.statistics, function (st) {
    return st.TABLE_NAME === tableName && st.INDEX_NAME !== 'PRIMARY';
  });
};

DBInfo.prototype.getColumnSQL = function (tableName, columnName) {
  if (_.isNil(tableName) || _.isNil(columnName)) {
    return null;
  }

  var tableSQL = this.createTableSQL[tableName];
  if (!tableSQL) {
    return null;
  }

  var lines = this.createTableSQL[tableName].split('\n');
  for (var i = 1; i < lines.length - 1; ++i) {
    if (lines[i].substr(3, columnName.length) === columnName) {
      return lines[i].substr(2, _.last(lines[i]) === ',' ? lines[i].length - 3 : lines[i].length - 2);
    }
  }

  return null;
};

DBInfo.prototype.getPrimariesSQL = function (tableName) {
  if (_.isNil(tableName)) {
    return null;
  }

  var tableSQL = this.createTableSQL[tableName];
  if (!tableSQL) {
    return null;
  }

  var lines = this.createTableSQL[tableName].split('\n');
  for (var i = 1; i < lines.length - 1; ++i) {
    if (lines[i].substr(2, 'PRIMARY KEY'.length) === 'PRIMARY KEY') {
      return lines[i].substr(2, _.last(lines[i]) === ',' ? lines[i].length - 3 : lines[i].length - 2);
    }
  }

  return null;
};

DBInfo.prototype.getKeySQL = function (tableName, keyName) {
  if (_.isNil(tableName) || _.isNil(keyName)) {
    return null;
  }

  var tableSQL = this.createTableSQL[tableName];
  if (!tableSQL) {
    return null;
  }

  var lines = this.createTableSQL[tableName].split('\n');
  for (var i = 1; i < lines.length - 1; ++i) {
    var line = lines[i];
    if (_.startsWith(line, '  KEY `' + keyName + '`')
      || _.startsWith(line, '  UNIQUE KEY `' + keyName + '`')
      || _.startsWith(line, '  FULLTEXT KEY `' + keyName + '`')
      || _.startsWith(line, '  SPATIAL KEY `' + keyName + '`')) {
      return line.substr(2, _.last(lines[i]) === ',' ? lines[i].length - 3 : lines[i].length - 2);
    }
  }

  return null;
};

DBInfo.prototype.getTableEngineAndSettingSQL = function (tableName) {
  var engineSQL = _.last(this.createTableSQL[tableName].split('\n'));
  return engineSQL.substr(2, engineSQL.length - 1);
};


var Schema = module.exports;

Schema.init = function (cb) {
  var self = this;

  self.devInfo = new DBInfo();
  self.proInfo = new DBInfo();

  async.parallel([
    function (cb) {
      self.devInfo.init(MysqlConfig.development, cb);
    },
    function (cb) {
      self.proInfo.init(MysqlConfig.production, cb);
    }
  ], function (err) {
    if (err) {
      Utils.colorPrint('init error:' + err.stack, Utils.STYLES.red);
      return;
    }

    Utils.invokeCallback(cb, err);
  });
};

Schema.getNewTables = function () {
  return _.difference(this.devInfo.getTableNames(), this.proInfo.getTableNames());
};

Schema.getDeletedTables = function () {
  return _.difference(this.proInfo.getTableNames(), this.devInfo.getTableNames());
};

Schema.getMayModifiedTables = function () {
  return _.intersection(this.devInfo.getTableNames(), this.proInfo.getTableNames());
};

Schema.getTableNewColumns = function (tableName) {
  var devColumns = this.devInfo.getTableColumnInfo(tableName);
  var proColumns = this.proInfo.getTableColumnInfo(tableName);

  return _.difference(_.map(devColumns, 'COLUMN_NAME'), _.map(proColumns, 'COLUMN_NAME'));
};

Schema.getTableDeletedColumns = function (tableName) {
  var devColumns = this.devInfo.getTableColumnInfo(tableName);
  var proColumns = this.proInfo.getTableColumnInfo(tableName);

  return _.difference(_.map(proColumns, 'COLUMN_NAME'), _.map(devColumns, 'COLUMN_NAME'));
};

Schema.getTableMayModifiedColumns = function (tableName) {
  var devColumns = this.devInfo.getTableColumnInfo(tableName);
  var proColumns = this.proInfo.getTableColumnInfo(tableName);

  return _.intersection(_.map(devColumns, 'COLUMN_NAME'), _.map(proColumns, 'COLUMN_NAME'));
};

Schema.getTableNewKeys = function (tableName) {
  var devKeys = this.devInfo.getTableKeyInfo(tableName);
  var proKeys = this.proInfo.getTableKeyInfo(tableName);

  return _.difference(_.uniq(_.map(devKeys, 'INDEX_NAME')), _.uniq(_.map(proKeys, 'INDEX_NAME')));
};

Schema.getTableDeletedKeys = function (tableName) {
  var devKeys = this.devInfo.getTableKeyInfo(tableName);
  var proKeys = this.proInfo.getTableKeyInfo(tableName);

  return _.difference(_.uniq(_.map(proKeys, 'INDEX_NAME')), _.uniq(_.map(devKeys, 'INDEX_NAME')));
};

Schema.getTableMayModifiedKeys = function (tableName) {
  var devKeys = this.devInfo.getTableKeyInfo(tableName);
  var proKeys = this.proInfo.getTableKeyInfo(tableName);

  return _.intersection(_.uniq(_.map(proKeys, 'INDEX_NAME')), _.uniq(_.map(devKeys, 'INDEX_NAME')));
};

Schema.end = function () {
  this.devInfo.connect.end();
  this.proInfo.connect.end();
};