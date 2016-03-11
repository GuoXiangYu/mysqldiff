#!/usr/bin/env node

var fs = require('fs');
var util = require('util');
var path = require('path');
var async = require('async');
var mysql = require('mysql');
var mkdirp = require('mkdirp');
var _ = require('underscore');

var mysqldiff = module.exports = {};

mysqldiff.invokeCallback = function (cb) {
  if (!!cb && typeof cb === 'function') {
    cb.apply(null, Array.prototype.slice.call(arguments, 1));
  }
};

mysqldiff.write = function (filePath, sql) {
  var str = util.format("\n%s\n", sql);
  console.log(str);

  var dirName = path.dirname(filePath);
  if (!fs.exists(dirName)) {
    mkdirp.sync(dirName);
  }

  fs.appendFileSync(filePath, str);
};

mysqldiff.isStringType = function (dataType) {
  return dataType === 'char' || dataType === 'varchar' ||
    dataType === 'blob' || dataType === 'tinyblob' ||
    dataType === 'mediumblob' || dataType === 'longblob' ||
    dataType === 'tinytext' || dataType === 'text' ||
    dataType === 'mediumtext' || dataType === 'longtext' ||
    dataType === 'varbinary' || dataType === 'binary';
};

mysqldiff.getComment = function (comment) {
  return comment.split(';')[0];
};

mysqldiff.getColumnString = function (colInfo) {
  var str = '';
  str += util.format('`%s`', colInfo.COLUMN_NAME);
  if (colInfo.COLUMN_TYPE) str += util.format('%s', colInfo.COLUMN_TYPE);
  if (colInfo.CHARACTER_SET_NAME) str += util.format(" CHARACTER SET %s", colInfo.CHARACTER_SET_NAME);
  if (colInfo.COLLATION_NAME) str += util.format(" COLLATE %s", colInfo.COLLATION_NAME);
  if (colInfo.IS_NULLABLE === 'NO') str += ' NOT NULL';
  if (colInfo.COLUMN_DEFAULT) {
    var defaultValue = mysqldiff.isStringType(colInfo.DATA_TYPE)
      ? util.format("'%s'", colInfo.COLUMN_DEFAULT)
      : colInfo.COLUMN_DEFAULT;
    str += util.format(' DEFAULT %s', defaultValue);
  }
  if (colInfo.COLUMN_COMMENT) str += util.format(" COMMENT '%s'", colInfo.COLUMN_COMMENT);
  return str;
};

mysqldiff.getStatisticsInfo = function (indexName, statInfoArray) {
  if (_.isEmpty(statInfoArray)) return '';

  var colNames = _.pluck(statInfoArray, 'COLUMN_NAME');
  var namesString = _.map(colNames, function (col) {
    return util.format('`%s`', col);
  }).join(',');

  if (indexName === 'PRIMARY') {
    return util.format('PRIMARY KEY (%s)', namesString);
  } else {
    return util.format('INDEX `%s` USING %s (%s)', indexName, statInfoArray[0].INDEX_TYPE, namesString);
  }
};

mysqldiff.createDeleteSql = function (dbName, tableName, filePath) {
  var sql = util.format("-- Delete table %s \nDROP TABLE `%s`.`%s`;", tableName, dbName, tableName);
  mysqldiff.write(filePath, sql);
};

mysqldiff.createInsertSql = function (conn, tableInfo, filePath, callback) {
  var dbName = conn.config.database;
  var tableName = tableInfo.TABLE_NAME;

  async.parallel([
    function (cb) {
      conn.query(
        "Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [dbName, tableName], cb);
    },
    function (cb) {
      conn.query(
        "Select * From INFORMATION_SCHEMA.STATISTICS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [dbName, tableName], cb);
    }
  ], function (err, results) {
    if (err) {
      mysqldiff.invokeCallback(callback, err);
      return;
    }

    var cloumnsInfo = results[0][0];
    var statistInfo = results[1][0];

    var strArray = [];

    cloumnsInfo.forEach(function (cloInfo) {
      strArray.push("  " + mysqldiff.getColumnString(cloInfo));
    });

    var statGroup = _.groupBy(statistInfo, function (info) {
      return info.INDEX_NAME;
    });
    for (var indexName in statGroup) {
      strArray.push("  " + mysqldiff.getStatisticsInfo(indexName, statGroup[indexName]));
    }

    var comment = util.format("-- New table %s \n", tableName);
    var prefix = util.format("CREATE TABLE `%s`.`%s`(\n", conn2.config.database, tableName);
    var createOptions = _.isEmpty(tableInfo.CREATE_OPTIONS) ? '' : tableInfo.CREATE_OPTIONS;
    var suffix = util.format("\n) ENGINE=`%s` COLLATE %s COMMENT='%s' %s;", tableInfo.ENGINE, tableInfo.TABLE_COLLATION,
      mysqldiff.getComment(tableInfo.TABLE_COMMENT), createOptions);
    var sql = util.format('%s%s%s%s', comment, prefix, strArray.join(",\n"), suffix);
    mysqldiff.write(filePath, sql);

    mysqldiff.invokeCallback(callback, null);
  });
};

mysqldiff.compareCommonTable = function (conn1, conn2, tableInfo1, tableInfo2, filePath, callback) {
  var db1Name = conn1.config.database;
  var db2Name = conn2.config.database;
  var tableName = tableInfo1.TABLE_NAME;

  var compareTableInfo = function () {
    var str = '';
    if (tableInfo1.ENGINE !== tableInfo2.ENGINE) str += util.format(' ENGINE=`%s`', tableInfo1.ENGINE);
    if (tableInfo1.TABLE_COLLATION !== tableInfo2.TABLE_COLLATION) str += util.format(' COLLATE %s', tableInfo1.TABLE_COLLATION);
    if (tableInfo1.CREATE_OPTIONS !== tableInfo2.CREATE_OPTIONS) str += util.format(' %s', tableInfo1.CREATE_OPTIONS);
    if (mysqldiff.getComment(tableInfo1.TABLE_COMMENT) !== mysqldiff.getComment(tableInfo2.TABLE_COMMENT)) {
      str += util.format(" COMMENT='%s'", tableInfo1.TABLE_COMMENT);
    }
    if (!_.isEmpty(str)) {
      return util.format('ALTER TABLE `%s`.`%s` %s;', db2Name, tableName, str);
    }
    return str;
  };

  // compare table info
  var tableInfoSql = compareTableInfo();
  if (!_.isEmpty(tableInfoSql)) {
    mysqldiff.write(filePath, tableInfoSql);
  }

  async.parallel([
    function (cb) {
      conn1.query(
        "Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db1Name, tableName], cb);
    },
    function (cb) {
      conn1.query(
        "Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db2Name, tableName], cb);
    }
  ], function (err, results) {
    if (err) {
      mysqldiff.invokeCallback(callback, err);
      return;
    }

    // compare column info
    var columnsInfo1 = results[0][0];
    var columnsInfo2 = results[1][0];

    var columnsName1 = _.pluck(columnsInfo1, 'COLUMN_NAME');
    var columnsName2 = _.pluck(columnsInfo2, 'COLUMN_NAME');

    var addColumnsName = _.difference(columnsName1, columnsName2);
    var dropColumnsName = _.difference(columnsName2, columnsName1);
    var commonColumnsName = _.intersection(columnsName1, columnsName2);

    var strArray = [];
    addColumnsName.forEach(function (columnName) {
      var info = _.findWhere(columnsInfo1, {'COLUMN_NAME': columnName});
      var str = util.format("ADD COLUMN %s", mysqldiff.getColumnString(info));
      var index = _.indexOf(columnsName1, columnName);
      if (index === 0) {
        str += " FIRST";
      } else {
        str += util.format(" AFTER `%s`", columnsName1[index - 1]);
      }
      if (!_.isEmpty(str)) strArray.push(str);
    });

    dropColumnsName.forEach(function (columnName) {
      strArray.push(util.format("DROP COLUMN `%s`", columnName));
    });

    commonColumnsName.forEach(function (columnName) {
      var col1 = _.findWhere(columnsInfo1, {'COLUMN_NAME': columnName});
      var col2 = _.findWhere(columnsInfo2, {'COLUMN_NAME': columnName});
      if (col1.ORDINAL_POSITION !== col2.ORDINAL_POSITION
        || col1.COLUMN_DEFAULT !== col2.COLUMN_DEFAULT
        || col1.IS_NULLABLE !== col2.IS_NULLABLE
        || col1.COLUMN_TYPE !== col2.COLUMN_TYPE
        || col1.COLUMN_COMMENT !== col2.COLUMN_COMMENT) {
        var str = util.format("CHANGE COLUMN `%s` %s", columnName, mysqldiff.getColumnString(col1));
        var index = _.indexOf(columnsName1, columnName);
        if (index === 0) {
          str += " FIRST";
        } else {
          str += util.format(" AFTER `%s`", columnsName1[index - 1]);
        }
        strArray.push(str);
      }
    });

    if (strArray.length !== 0) {
      var sql = util.format('ALTER TABLE `%s`.`%s` %s;', db2Name, tableName, strArray.join(','));
      mysqldiff.write(filePath, sql);
    }

    mysqldiff.invokeCallback(callback, null);
  });
};

if (module.id === require.main.id) {
  // get db config
  var dbConfigFile = process.argv[2] || './config.json';
  var saveFilePath = process.argv[3] || path.join('./sql', (new Date()).toISOString() + '.sql');
  console.log('Database config file path:', path.resolve(dbConfigFile));
  console.log('SQL string saving file path:', path.resolve(saveFilePath));

  var config = require(dbConfigFile);
  var db1Name = config.development.database;
  var db2Name = config.production.database;

  var conn1 = mysql.createConnection(config.development);
  var conn2 = mysql.createConnection(config.production);

  async.parallel([
    function (callback) {
      conn1.query("Select * From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA = ?", db1Name, callback);
    },
    function (callback) {
      conn2.query("Select * From INFORMATION_SCHEMA.TABLES Where TABLE_SCHEMA = ?", db2Name, callback);
    }
  ], function (err, results) {
    if (err) {
      mysqldiff.invokeCallback(callback, err);
      return;
    }
    var db1Tables = results[0][0];
    var db2Tables = results[1][0];

    var db1TablesName = _.pluck(db1Tables, 'TABLE_NAME');
    var db2TablesName = _.pluck(db2Tables, 'TABLE_NAME');

    var insertTablesName = _.difference(db1TablesName, db2TablesName);
    var deleteTablesName = _.difference(db2TablesName, db1TablesName);
    var commonTablesName = _.intersection(db1TablesName, db2TablesName);

    deleteTablesName.forEach(function (tableName) {
      mysqldiff.createDeleteSql(db2Name, tableName, saveFilePath);
    });

    async.series([
      function (callback) {
        async.forEachOfSeries(insertTablesName, function (tableName, idx, cb) {
          var tableInfo = _.findWhere(db1Tables, {'TABLE_NAME': tableName});
          mysqldiff.createInsertSql(conn1, tableInfo, saveFilePath, cb);
        }, callback);
      },
      function (callback) {
        async.forEachOfSeries(commonTablesName, function (tableName, idx, cb) {
          var tableInfo1 = _.findWhere(db1Tables, {'TABLE_NAME': tableName});
          var tableInfo2 = _.findWhere(db2Tables, {'TABLE_NAME': tableName});
          mysqldiff.compareCommonTable(conn1, conn2, tableInfo1, tableInfo2, saveFilePath, cb);
        }, callback);
      }
    ], function (err) {
      if (!err) {
        console.log('Compare finished.');
        //console.log('New tables: ', insertTablesName.length === 0 ? 'Not new table' : insertTablesName.join(','));
        //console.log('Drop tables: ', deleteTablesName.length === 0 ? 'Not drop table' : deleteTablesName.join(','));
      } else {
        console.log('Error: ', err);
      }

      // close mysql connection
      conn1.end();
      conn2.end();
    });
  });
}