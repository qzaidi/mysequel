mysequel
--------

anydb-sql, without the portability layer.

Motivation
----------

This package combines mysql and sql in much the same way as anydb-sql does for anydb and sql.
With too many packages/middlewares, it was becoming impossible to control things, add debugging and keep
anydb up to date in most projects, and by doing away with all the middleware and using the mysql pool 
instead of generic pool, we intend to solve a lot of issues related to running out of db conns.

API
---

Tests
-----

To run the tests, modify the db credentials
