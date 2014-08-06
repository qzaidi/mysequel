"use strict";

var mysql = require('mysql');
var sql = require('sql');
var url = require('url');
var debug = require('debug')('mysequel'); // TODO: replace with bunyan

module.exports = function (opt) {
  var dialect = url.parse(opt.url).protocol;
  dialect = dialect.substr(0, dialect.length - 1);
  sql.setDialect('mysql');
};
