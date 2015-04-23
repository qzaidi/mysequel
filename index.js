"use strict";

var mysql = require('mysql');
var sql = require('sql');
var url = require('url');
var debug = require('debug')('mysequel:info');
var warn = require('debug')('mysequel:warn');

var queryMethods = [
                     'select', 'from', 'insert', 'update',
                     'delete', 'create', 'drop', 'alter', 
                     'where', 'indexes'];

var normalizer = function (row) {
  var res = {};
  Object.keys(row).forEach(function(key) {
    var path = key.split('.'), plen = path.length, k, item, obj;
    for (k = 0, obj = res; k < plen - 1; ++k) {
      item = path[k];
      if (!obj[item]) {
        obj[item] = {};
      }
      obj = obj[item];
    }
    item = path[plen - 1];
    obj[item] = row[key];
    });
  return res;
}

module.exports = function (opt) {
  var pool;
  var urlOpts = url.parse(opt.url);
  var auth = urlOpts.auth.split(':');
  var self = {};
  sql.setDialect('mysql');
  var timeout = opt.timeout || 60000;

  if (urlOpts.protocol != 'mysql:') {
    console.error('invalid dialect ' + urlOpts.protocol + ' in ' + opt.url);
  }

  self.open = function() {
    if (pool) {
      return;
    }

    warn('creating pool with ' + opt.connections.max + ' connections');
    pool  = mysql.createPool({
      connectionLimit : opt.connections.max || 10,
      host            : urlOpts.hostname,
      user            : auth[0],
      password        : auth[1],
      port            : urlOpts.port || 3306,
      database        : urlOpts.path.substring(1)
    });
  };

  self.open();

  self.models = {};

  function extendedQuery(query) {
    var extQuery = Object.create(query);
    var self = extQuery;

    self.__extQuery = true;

    extQuery.execWithin = function (where, nested, appended, fn) {
      var query = self.toQuery(); // {text, params}
      debug(query.text,query.values);
      if (!fn) {
        return where.query({ sql: query.text + appended, timeout: opt.timeout, nestTables: nested }, query.values);
      }
      return where.query({ sql: query.text + appended, timeout: opt.timeout, nestTables: nested }, query.values, function (err, res) {
        debug('responded to ' + query.text);
        var rows;
        if (err) {
          err = new Error(err);
          err.message = 'SQL' + err.message + '\n' + query.text  + appended
          + '\n' + query.values;
        }
        rows = res;
        fn(err, rows && rows.length && nested ? rows.map(normalizer) : rows);
      });
    };

    extQuery.exec = extQuery.execWithin.bind(extQuery, pool, false, '');
    extQuery.execNested = extQuery.execWithin.bind(extQuery, pool, '.','');

    extQuery.all = extQuery.exec;

    extQuery.get = function (fn) {
      return this.exec(function (err, rows) {
        return fn(err, rows && rows.length ? rows[0] : null);
      });
    };

    /**
    * Returns a result from a query, mapping it to an object by a specified key.
    * @param {!String} keyColumn the column to use as a key for the map.
    * @param {!Function} callback called when the operation ends. Takes an error and the result.
    * @param {String|Array|Function=} mapper can be:<ul>
    *     <li>the name of the column to use as a value;</li>
    *     <li>an array of column names. The value will be an object with the property names from this array mapped to the
    *         column values from the array;</li>
    *     <li>a function that takes the row as an argument and returns a value.</li>
    *  </ul>
    *                                        If omitted, assumes all other columns are values. If there is only one
    *                                        other column, its value will be used for the object. Otherwise, the
    *                                        value will be an object with the values mapped to column names.
    * @param {Function=} filter takes a row and returns a value indicating whether the row should be inserted in the
    *                           result.
    */
    extQuery.allObject = function(keyColumn, callback, mapper, filter) {
      filter = filter || function() { return true; };

      if (mapper) {
        if (typeof mapper === 'string') {
          var str = mapper;
          mapper = function(row) { return row[str]; };
        } else if (typeof mapper === 'object') {
          var arr = mapper;
          mapper = function(row) {
            var obj = {};
            var j;
            for (j = 0; j < arr.length; j++) {
              obj[arr[j]] = row[arr[j]];
            }
            return obj;
          };
        }
      } else mapper = function(row) {
        var validKeys = Object.keys(row).filter(function(key) { return key != keyColumn; });

        if (validKeys.length == 0) return null;
        else if (validKeys.length == 1) return row[validKeys[0]];
        else {
          var obj = {};
          var j;
          for (j = 0; j < validKeys.length; j++) obj[validKeys[j]] = row[validKeys[j]];
          return obj;
        }
      };

      return this.exec(function(err, data) {
        if (err) return callback(err);

        var result = {};
        var i;
        for (i = 0; i < data.length; i++) {
          if (filter(data[i])) {
            result[data[i][keyColumn]] = mapper(data[i]);
          }
        }

        callback(null, result);
      });
    };

    queryMethods.forEach(function (key) {
      extQuery[key] = function () {
        var q = query[key].apply(query, arguments);
        if (q.__extQuery) return q;
        return extendedQuery(q);
      }
    });

    return extQuery;
  }

  function extendedTable(table) {
    // inherit everything from a regular table.
    var extTable = Object.create(table); 

    // make query methods return extended queries.
    queryMethods.forEach(function (key) {
      extTable[key] = function () {
        return extendedQuery(table[key].apply(table, arguments));
      };
    });


    // make as return extended tables.
    extTable.as = function () {
      return extendedTable(table.as.apply(table, arguments));
    };
    return extTable;
  }


  self.define = function (opt) {
    var t = extendedTable(sql.define.apply(sql, arguments));
    self.models[opt.name] = t;
    return t;
  };

  self.functions = sql.functions;

  self.query = pool.query.bind(pool);
  self.getConnection = pool.getConnection.bind(pool)

  var msql_enqueuedSince = undefined;
  pool.on('enqueue', function () {
    if (msql_enqueuedSince == undefined) {
      msql_enqueuedSince = Date.now(); // first time
    } else {
      warn('Waiting for available connection slot max = ' + opt.connections.max, msql_enqueuedSince);
    }
  });

  pool.on('connection', function(connection) {
    var queueDuration;
    if (msql_enqueuedSince) {
      queueDuration = Date.now() - msql_enqueuedSince;
      warn('got connection after ', queueDuration);
      if (typeof(opt.overloadCB) == 'Function') {
        opt.overloadCB(queueDuration);
      }
      msql_enqueuedSince = undefined;
    }
  });

  // self.tooBusy can be used before accepting connections, to allow for failing early
  // e.g.
  // app.use(function(req,res,next) {
  //  if (db.tooBusy(1000)) {
  //    // fail if already waiting 1 second to get a db conn
  //    res.status(503).send('Too Busy');
  //  }
  //  next();
  // });
  self.tooBusy = function(threshold) {
    if (msql_enqueuedSince && msql_enqueuedSince - Date.now() > threshold) {
      return true;
    }
    return false;
  };

  return self;
};
