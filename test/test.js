var assert = require('assert');
var mysequel = require('../');

var db;
var infoSchema;
describe('Basic', function() {

  before(function(done) {
    var dbConfig = { 
      url: 'mysql://root:@:3306/information_schema',
      connections: { min: 1, max: 2 }
    };

    var schema = {
      'name': 'tables',
      'columns': [
        'table_catalog', 'table_schema', 'table_name', 'table_type'
      ]
    };

    db = mysequel(dbConfig);
    infoSchema = db.define(schema);

    db.getConnection(function(err,conn) {
      if (err) {
        console.log('failed to connect to db - using credentials ',dbConfig.url,err.message);
      }
      assert(err == undefined);
      conn.release();
      done();
    });
  })

  describe('#create pool', function() {
   it('should do select with limits', function(done) {
    infoSchema.select().limit(2).exec(function(err,rows) {
      assert(err == undefined && rows.length == 2);
      assert(rows[0].TABLE_SCHEMA == 'information_schema');
      done();
    });
   });

   it('should allow direct queries using .query', function(done) {
    db.query('select * from tables limit 1', function(err,rows) {
      assert(err == undefined && rows.length == 1);
      assert(rows[0].TABLE_SCHEMA == 'information_schema');
      done();
    });
   });

   it('should allow to get a connection and run query on it', function(done) {
    db.getConnection(function(err,connection) {
      assert(err == undefined);
      console.log('thread id ', connection.threadId);
      connection.query('select * from tables limit 1', function(err,rows) {
        assert(err == undefined && rows.length == 1);
        assert(rows[0].TABLE_SCHEMA == 'information_schema');
        connection.release();
        done();
      });
    });
   });

   it('should return tooBusy when out of conns', function(done) {
    db.getConnection(function(err,conn1) {
      var tooBusyCalled = false;
      console.log('blocked thread ', conn1.threadId);
      db.getConnection(function(err,conn) {
        console.log('blocked thread ', conn.threadId);
        db.query('select * from tables limit 1',function(err,rows) {
          assert(tooBusyCalled); // this callback will not be called.
        });

        var t = setTimeout(function() {
          if (db.tooBusy(500)) {
            console.log('too busy called');
            clearTimeout(t);
            tooBusyCalled = true;
          }
          conn.release();
          conn1.release();
          done();
        },1500);
    });
   });
  });

  });
})
