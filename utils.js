/**
 * Created by xiangyuguo on 17/1/24.
 */
'use strict';

var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');

var Utils = module.exports;

Utils.STYLES = {
  // styles
  'bold': [1, 22],
  'italic': [3, 23],
  'underline': [4, 24],
  'inverse': [7, 27],

  // grayscale
  'white': [37, 39],
  'grey': [90, 39],
  'black': [90, 39],

  // colors
  'blue': [34, 39],
  'cyan': [36, 39],
  'green': [32, 39],
  'magenta': [35, 39],
  'red': [31, 39],
  'yellow': [33, 39]
};

/**
 * 彩色打印
 * @param str
 * @param style
 */
Utils.colorPrint = function (str, style) {
  if (!style) {
    console.log(str);
  }

  var start = '\x1B[' + style[0] + 'm';
  var end = '\x1B[' + style[1] + 'm';
  console.log(start + str + end);
};

/**
 *
 * @param cb
 */
Utils.invokeCallback = function (cb) {
  if (!!cb && typeof cb === 'function') {
    cb.apply(null, Array.prototype.slice.call(arguments, 1));
  }
};

/**
 * 将内容写入到文件
 * @param filePath    {String}
 * @param content     {String}
 */
Utils.appendSQL = function (filePath, content) {
  var dirName = path.dirname(filePath);
  if (!fs.exists(dirName)) {
    mkdirp.sync(dirName);
  }

  fs.appendFileSync(filePath, content);
};
