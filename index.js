#!/usr/bin/env node

var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var mkdirp = require('mkdirp');
var Utils = require('./utils.js');

/**
 * 程序入口
 */
if (module.id === require.main.id) {
  var schema = require('./schema.js');
  schema.init(function (err) {
    if (err) {
      Utils.colorPrint('Schema init error:' + err.stack, Utils.STYLES.red);
      process.exit(1);
    }

    var sqlFile = Date.now() + '.sql';

    // 已删除表
    var deletedTables = schema.getDeletedTables();
    for (var i = 0; i < deletedTables.length; ++i) {
      Utils.appendSQL(sqlFile, util.format("DROP TABLE `%s`.`%s`;", schema.proInfo.getDbName(), deletedTables[i]));
    }
    if (deletedTables.length !== 0) {
      Utils.colorPrint('The deleted table names are: ' + deletedTables.join(', '), Utils.STYLES.yellow);
    }

    // 新建表
    var newTables = schema.getNewTables();
    for (var i = 0; i < newTables.length; ++i) {
      Utils.appendSQL(sqlFile, '\n\n' + schema.devInfo.createTableSQL[newTables[i]] + ';');
    }
    if (newTables.length !== 0) {
      Utils.colorPrint('The new table names are: ' + deletedTables.join(', '), Utils.STYLES.yellow);
    }

    // 修改表
    var modifiedTables = [];
    var mayModifiedTables = schema.getMayModifiedTables();
    for (var i = 0; i < mayModifiedTables.length; ++i) {
      var tableName = mayModifiedTables[i];
      var columnsName = schema.devInfo.getTableColumnNames(tableName);

      var newColumns = schema.getTableNewColumns(tableName);
      var deletedColumns = schema.getTableDeletedColumns(tableName);
      var mayModifiedColumns = schema.getTableMayModifiedColumns(tableName);

      // 删除字段
      var modifiedStringArray = [];
      for (var j = 0; j < deletedColumns.length; ++j) {
        modifiedStringArray.push(util.format("\n  DROP COLUMN `%s`", deletedColumns[j]));
      }

      // 新增字段
      for (var j = 0; j < newColumns.length; ++j) {
        var index = _.indexOf(columnsName, newColumns[j]);
        if (index === 0) {
          modifiedStringArray.push(util.format('\n  ADD COLUMN %s FIRST', schema.devInfo.getColumnSQL(tableName, newColumns[j])));
        } else {
          modifiedStringArray.push(util.format('\n  ADD COLUMN %s AFTER `%s`', schema.devInfo.getColumnSQL(tableName, newColumns[j]), columnsName[index - 1]));
        }
      }

      // 修改字段
      for (var j = 0; j < mayModifiedColumns.length; ++j) {
        var devColumnSQL = schema.devInfo.getColumnSQL(tableName, mayModifiedColumns[j]);
        var proColumnSQL = schema.proInfo.getColumnSQL(tableName, mayModifiedColumns[j]);
        if (devColumnSQL !== proColumnSQL) {
          modifiedStringArray.push(util.format('\n  CHANGE COLUMN `%s` %s', mayModifiedColumns[j], devColumnSQL));
        }
      }

      // 主键
      var devPrimarySQL = schema.devInfo.getPrimariesSQL(tableName);
      var proPrimarySQL = schema.proInfo.getPrimariesSQL(tableName);
      if (devPrimarySQL !== proPrimarySQL) {
        modifiedStringArray.push(util.format('\n  DROP PRIMARY KEY, ADD %s', devPrimarySQL));
      }

      // 索引
      var deletedKeys = schema.getTableDeletedKeys(tableName);
      for (var j = 0; j < deletedKeys.length; ++j) {
        modifiedStringArray.push(util.format('\n  DROP KEY `%s`', deletedKeys[j]));
      }

      var newKeys = schema.getTableNewKeys(tableName);
      for (var j = 0; j < newKeys.length; ++j) {
        modifiedStringArray.push(util.format('\n  ADD %s', schema.devInfo.getKeySQL(tableName, newKeys[j])));
      }

      var mayModifiedKeys = schema.getTableMayModifiedKeys(tableName);
      for (var j = 0; j < mayModifiedKeys.length; ++j) {
        var devKeySQL = schema.devInfo.getKeySQL(tableName, mayModifiedKeys[j]);
        var proKeySQL = schema.proInfo.getKeySQL(tableName, mayModifiedKeys[j]);
        if (devKeySQL !== proKeySQL) {
          modifiedStringArray.push(util.format('\n  DROP KEY `%s`', mayModifiedKeys[j]));
          modifiedStringArray.push(util.format('\n  ADD %s', devKeySQL));
        }
      }

      // 设置
      var devEngineSQL = schema.devInfo.getTableEngineAndSettingSQL(tableName);
      var proEngineSQL = schema.proInfo.getTableEngineAndSettingSQL(tableName);
      if (devEngineSQL !== proEngineSQL) {
        modifiedStringArray.push(util.format('\n  %s', devEngineSQL));
      }

      if (modifiedStringArray.length !== 0) {
        modifiedTables.push(tableName);
        Utils.appendSQL(sqlFile, util.format("\n\nALTER TABLE `%s`.`%s`", schema.proInfo.getDbName(), tableName));
        Utils.appendSQL(sqlFile, modifiedStringArray.join(',') + ';');
      }
    }
    if (modifiedTables.length !== 0) {
      Utils.colorPrint('The altered table names are: ' + modifiedTables.join(', '), Utils.STYLES.yellow);
    }

    schema.end();
  });
}