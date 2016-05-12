#!/usr/bin/env node

var fs = require('fs');
var util = require('util');
var path = require('path');
var async = require('async');
var mysql = require('mysql');
var mkdirp = require('mkdirp');
var _ = require('lodash');

var mysqldiff = module.exports = {};

/**
 * 执行回调
 * @param cb
 */
mysqldiff.invokeCallback = function (cb) {
  if (!!cb && typeof cb === 'function') {
    cb.apply(null, Array.prototype.slice.call(arguments, 1));
  }
};

/**
 * 写文件
 * @param filePath
 * @param sql
 */
mysqldiff.write = function (filePath, sql) {
  var dirName = path.dirname(filePath);
  if (!fs.exists(dirName)) {
    mkdirp.sync(dirName);
  }

  fs.appendFileSync(filePath, util.format("\n%s\n", sql));
};

/**
 * 判断字段类型是否是字符串
 * @param dataType
 * @returns {boolean}
 */
mysqldiff.isStringType = function (dataType) {
  return dataType === 'char' || dataType === 'varchar' ||
    dataType === 'blob' || dataType === 'tinyblob' ||
    dataType === 'mediumblob' || dataType === 'longblob' ||
    dataType === 'tinytext' || dataType === 'text' ||
    dataType === 'mediumtext' || dataType === 'longtext' ||
    dataType === 'varbinary' || dataType === 'binary';
};

/**
 * 取表注释，过滤掉多余部分
 * @param comment
 * @returns {string|*|{arity, flags, keyStart, keyStop, step}}
 */
mysqldiff.getComment = function (comment) {
  comment = comment.replace(/\n/g, '');
  var index = comment.lastIndexOf(';');
  return comment.substr(0, index);
};

/**
 * 转换列信息为sql
 * @param columnInfo
 * @returns {string}
 */
mysqldiff.getColumnString = function (columnInfo) {
  // 默认值转换
  var defaultValue = mysqldiff.isStringType(columnInfo.DATA_TYPE) ?
    util.format("'%s'", columnInfo.COLUMN_DEFAULT) :
    columnInfo.COLUMN_DEFAULT;

  // 字段名-类型-字符集-字符集子类-是否可空-默认值-注释
  return util.format('`%s`%s%s%s%s%s%s',
    columnInfo.COLUMN_NAME,
    columnInfo.COLUMN_TYPE,
    columnInfo.CHARACTER_SET_NAME ? " CHARACTER SET " + columnInfo.CHARACTER_SET_NAME : '',
    columnInfo.COLLATION_NAME ? " COLLATE " + columnInfo.COLLATION_NAME : '',
    columnInfo.IS_NULLABLE === 'NO' ? ' NOT NULL' : '',
    columnInfo.COLUMN_DEFAULT !== null ? ' DEFAULT ' + defaultValue : '',
    columnInfo.COLUMN_COMMENT ? util.format(" COMMENT '%s'", columnInfo.COLUMN_COMMENT) : ''
  );
};

/**
 * 转换主键和索引
 * @param indexName
 * @param statInfoArray
 * @returns {*}
 */
mysqldiff.getStatisticsInfo = function (indexName, statInfoArray) {
  if (_.isEmpty(statInfoArray)) return '';

  var columnNames = _.map(statInfoArray, 'COLUMN_NAME');
  var namesString = _.map(columnNames, function (column) {
    return util.format('`%s`', column);
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

/**
 * 根据表信息生成新表sql
 * @param conn        db连接对象，用于获取列、主键、索引信息
 * @param tableInfo   表信息
 * @param filePath    保存到文件
 * @param callback
 */
mysqldiff.createInsertSql = function (conn, tableInfo, filePath, callback) {
  var dbName = conn.config.database;
  var tableName = tableInfo.TABLE_NAME;

  // 获取列、主键、索引数据
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

    var contents = [];

    // 生成列sql
    cloumnsInfo.forEach(function (cloInfo) {
      contents.push("  " + mysqldiff.getColumnString(cloInfo));
    });

    // 生成主键、索引sql
    var statGroup = _.groupBy(statistInfo, function (info) {
      return info.INDEX_NAME;
    });
    for (var indexName in statGroup) {
      contents.push("  " + mysqldiff.getStatisticsInfo(indexName, statGroup[indexName]));
    }

    // 拼接sql
    var formatString = "CREATE TABLE `%s`.`%s`(\n%s\n) ENGINE=`%s` COLLATE %s COMMENT='%s' %s;";
    var sql = util.format(formatString,
      conn2.config.database,
      tableName,
      contents.join(",\n"),
      tableInfo.ENGINE,
      tableInfo.TABLE_COLLATION,
      mysqldiff.getComment(tableInfo.TABLE_COMMENT),
      _.isEmpty(tableInfo.CREATE_OPTIONS) ? '' : tableInfo.CREATE_OPTIONS);

    mysqldiff.write(filePath, sql);

    console.log('=====\t\tNEW TABLE ' + tableName + '\t\t=====');
    console.log(sql);
    console.log('\n');

    mysqldiff.invokeCallback(callback, null);
  });
};

/**
 *
 * @param conn1
 * @param conn2
 * @param tableInfo1
 * @param tableInfo2
 * @param filePath
 * @param callback
 */
mysqldiff.compareCommonTable = function (conn1, conn2, tableInfo1, tableInfo2, filePath, callback) {
  var db1Name = conn1.config.database;
  var db2Name = conn2.config.database;
  var tableName = tableInfo1.TABLE_NAME;

  // 比对表信息
  var compareTableInfo = function () {
    var sql = util.format('%s%s%s',
      tableInfo1.ENGINE !== tableInfo2.ENGINE ? util.format(' ENGINE=`%s`', tableInfo1.ENGINE) : '',
      tableInfo1.TABLE_COLLATION !== tableInfo2.TABLE_COLLATION ? ' COLLATE ' + tableInfo1.TABLE_COLLATION : '',
      tableInfo1.CREATE_OPTIONS !== tableInfo2.CREATE_OPTIONS ? ' ' + tableInfo1.CREATE_OPTIONS : ''
      //mysqldiff.getComment(tableInfo1.TABLE_COMMENT) !== mysqldiff.getComment(tableInfo2.TABLE_COMMENT) ?
      //  util.format(" COMMENT='%s'", mysqldiff.getComment(tableInfo1.TABLE_COMMENT)) : ''
    );
    return _.isEmpty(sql) ? '' : util.format('ALTER TABLE `%s`.`%s` %s;', db2Name, tableName, sql);
  };

  console.log('=====\t\tCHANGE TABLE ' + tableName + '\t\t =====');
  var tableInfoSql = compareTableInfo();
  if (!_.isEmpty(tableInfoSql)) {
    console.log(tableInfoSql);
    mysqldiff.write(filePath, tableInfoSql);
  }

  async.parallel([
    function (cb) {
      conn1.query(
        "Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db1Name, tableName], cb);
    },
    function (cb) {
      conn2.query(
        "Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db2Name, tableName], cb);
    },
    function (cb) {
      conn1.query(
        "Select * From INFORMATION_SCHEMA.STATISTICS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db1Name, tableName], cb);
    },
    function (cb) {
      conn2.query(
        "Select * From INFORMATION_SCHEMA.STATISTICS Where TABLE_SCHEMA = ? And TABLE_NAME = ?", [db2Name, tableName], cb);
    }
  ], function (err, results) {
    if (err) {
      mysqldiff.invokeCallback(callback, err);
      return;
    }

    var diffSql = [];

    // 比对字段
    var columnsInfo1 = _.sortBy(results[0][0], function (c) {
      return c.ORDINAL_POSITION;
    });
    var columnsInfo2 = _.sortBy(results[1][0], function (c) {
      return c.ORDINAL_POSITION;
    });

    var columnsName1 = _.map(columnsInfo1, 'COLUMN_NAME');
    var columnsName2 = _.map(columnsInfo2, 'COLUMN_NAME');

    var addColumnsName = _.difference(columnsName1, columnsName2);
    var dropColumnsName = _.difference(columnsName2, columnsName1);
    var commonColumnsName = _.intersection(columnsName1, columnsName2);

    addColumnsName.forEach(function (columnName) {
      var str = '';
      var info = _.find(columnsInfo1, {'COLUMN_NAME': columnName});
      var index = _.indexOf(columnsName1, columnName);
      if (index === 0) {
        str = util.format("ADD COLUMN %s FIRST", mysqldiff.getColumnString(info));
      } else {
        str = util.format("ADD COLUMN %s AFTER `%s`", mysqldiff.getColumnString(info), columnsName1[index - 1]);
      }
      if (!_.isEmpty(str)) diffSql.push(str);
    });

    // TODO 不处理删除字段
    //dropColumnsName.forEach(function (columnName) {
    //  diffSql.push(util.format("DROP COLUMN `%s`", columnName));
    //});

    commonColumnsName.forEach(function (columnName) {
      var col1 = _.find(columnsInfo1, {'COLUMN_NAME': columnName});
      var col2 = _.find(columnsInfo2, {'COLUMN_NAME': columnName});

      // 类型、默认值、是否为空、描述不同，则生成修改sql
      if (col1.COLUMN_TYPE !== col2.COLUMN_TYPE ||
        col1.COLUMN_DEFAULT !== col2.COLUMN_DEFAULT ||
        col1.IS_NULLABLE !== col2.IS_NULLABLE) { // ||
        //col1.COLUMN_COMMENT !== col2.COLUMN_COMMENT) {
        var str = '';
        var index = _.indexOf(columnsName1, columnName);
        if (index === 0) {
          str = util.format("CHANGE COLUMN `%s` %s FIRST", columnName, mysqldiff.getColumnString(col1));
        } else {
          str = util.format("CHANGE COLUMN `%s` %s AFTER `%s`", columnName, mysqldiff.getColumnString(col1), columnsName1[index - 1]);
        }
        diffSql.push(str);
      }
    });

    // 比对主键和索引
    var statInfo1 = results[2][0];
    var statInfo2 = results[3][0];

    columnsName1 = _.map(statInfo1, 'COLUMN_NAME');
    columnsName2 = _.map(statInfo2, 'COLUMN_NAME');

    var addStatName = _.difference(columnsName1, columnsName2);
    var dropStatName = _.difference(columnsName2, columnsName1);
    var commonStatName = _.intersection(columnsName1, columnsName2);

    // 新增主键、索引
    addStatName.forEach(function (columnName) {
      var sql = '';
      var info = _.find(statInfo1, {'COLUMN_NAME': columnName});
      var columns = _.filter(statInfo1, function (e) {
        return e.INDEX_NAME === info.INDEX_NAME;
      });

      if (info.INDEX_NAME === 'PRIMARY') {
        // 查找是否有相同主键，有则删除旧主键
        if (_.find(statInfo2, {'INDEX_NAME': info.INDEX_NAME}) !== void 0) {
          sql += 'DROP PRIMARY KEY, ';
        }
        // 添加新主键
        sql += util.format('ADD PRIMARY KEY (%s)', _.map((columns), function (c) {
          return util.format('`%s`', c.COLUMN_NAME);
        }).join(','));

      } else {
        // 查找是否有相同索引，有则删除旧索引
        if (_.find(statInfo2, {'INDEX_NAME': info.INDEX_NAME}) !== void 0) {
          sql += util.format('DROP INDEX `%s`, ', info.INDEX_NAME);
        }
        // 添加新索引
        sql += util.format('ADD INDEX `%s` USING %s (%s)', info.INDEX_NAME, info.INDEX_TYPE,
          _.map((columns), function (c) {
            return util.format('`%s`', c.COLUMN_NAME);
          }).join(','));
      }
      diffSql.push(sql);
    });

    // 删除主键、索引
    dropStatName.forEach(function (columnName) {
      var sql = '';
      var info = _.find(statInfo2, {'COLUMN_NAME': columnName});
      var columns = _.filter(statInfo1, function (e) {
        return e.INDEX_NAME === info.INDEX_NAME;
      });
      if (info.INDEX_NAME === 'PRIMARY') {
        sql += 'DROP PRIMARY KEY';

        // 还有其他主键，则添加删除后的主键
        if (_.find(statInfo2, {'INDEX_NAME': info.INDEX_NAME}) !== void 0) {
          sql += util.format(', ADD PRIMARY KEY (%s)', _.map((columns), function (c) {
            return util.format('`%s`', c.COLUMN_NAME);
          }).join(','));
        }
      } else {
        sql += util.format('DROP INDEX `%s`', info.INDEX_NAME);

        if (_.find(statInfo2, {'INDEX_NAME': info.INDEX_NAME}) !== void 0) {
          sql += util.format(', ADD INDEX `%s` USING %s (%s)', info.INDEX_NAME, info.INDEX_TYPE,
            _.map((columns), function (c) {
              return util.format('`%s`', c.COLUMN_NAME);
            }).join(','));
        }
      }
      diffSql.push(sql);
    });

    // 写入文件
    if (diffSql.length !== 0) {
      var sql = util.format('ALTER TABLE `%s`.`%s` %s;', db2Name, tableName, diffSql.join(','));
      mysqldiff.write(filePath, sql);
      console.log(sql);
    } else if (_.isEmpty(tableInfoSql)) {
      console.log('no changed');
    }
    console.log('\n');

    mysqldiff.invokeCallback(callback, null);
  });
};

/**
 * 程序入口
 */
if (module.id === require.main.id) {
  // 获取数据库配置与保存路径
  var dbConfigFile = process.argv[2] || './config.json';
  var saveFilePath = process.argv[3] || path.join('./sql', (new Date()).toISOString() + '.sql');
  console.log('Database config file path:', path.resolve(dbConfigFile));
  console.log('SQL string saving file path:', path.resolve(saveFilePath));
  console.log('\n');

  var config = require(dbConfigFile);
  var db1Name = config.development.database;
  var db2Name = config.production.database;

  // 创建数据库连接对象
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
      console.log('connection db error:', err);
      if (conn1) conn1.end();
      if (conn2) conn2.end();
      return;
    }
    var db1Tables = results[0][0];
    var db2Tables = results[1][0];

    var db1TablesName = _.map(db1Tables, 'TABLE_NAME');
    var db2TablesName = _.map(db2Tables, 'TABLE_NAME');

    // 获取新增、删除的表
    var insertTablesName = _.difference(db1TablesName, db2TablesName);
    var deleteTablesName = _.difference(db2TablesName, db1TablesName);
    var commonTablesName = _.intersection(db1TablesName, db2TablesName);

    // TODO 不移除表
    //deleteTablesName.forEach(function (tableName) {
    //  mysqldiff.createDeleteSql(db2Name, tableName, saveFilePath);
    //});

    async.series([
      function (callback) {
        async.forEachOfSeries(insertTablesName, function (tableName, idx, cb) {
          var tableInfo = _.find(db1Tables, {'TABLE_NAME': tableName});
          mysqldiff.createInsertSql(conn1, tableInfo, saveFilePath, cb);
        }, callback);
      },
      function (callback) {
        async.forEachOfSeries(commonTablesName, function (tableName, idx, cb) {
          var tableInfo1 = _.find(db1Tables, {'TABLE_NAME': tableName});
          var tableInfo2 = _.find(db2Tables, {'TABLE_NAME': tableName});
          mysqldiff.compareCommonTable(conn1, conn2, tableInfo1, tableInfo2, saveFilePath, cb);
        }, callback);
      }
    ], function (err) {
      if (!err) {
        console.log('Compare finished.');
      } else {
        console.log('Error: ', err);
      }

      // close mysql connection
      conn1.end();
      conn2.end();
    });
  });
}