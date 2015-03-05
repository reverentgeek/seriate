/* global describe,before,it */
var expect = require( "expect.js" );
var sinon = require( "sinon" );
expect = require( "sinon-expect" ).enhance( expect, sinon, "was" );

var sql = require( "../../src/index.js" );
var config = require( "./local-config.json" );
var getRowId = ( function() {
	var _id = 0;
	return function() {
		return _id++;
	};
}());

describe( "Seriate Integration Tests", function() {
	before( function( done ) {
		this.timeout( 20000 );
		sql.getPlainContext( config )
			.step( "DropDatabase", {
				query: "if db_id('tds_node_test') is not null drop database tds_node_test"
			} )
			.step( "CreateDatabase", {
				query: "create database tds_node_test"
			} )
			.step( "CreateTable", {
				query: "create table tds_node_test..NodeTestTable (bi1 bigint not null identity(1,1) primary key, v1 varchar(255), i1 int null)"
			} ).step( "CreateSecondTable", {
			query: "create table tds_node_test..NodeTestTableNoIdent (bi1 bigint not null primary key, v1 varchar(255), i1 int null)"
		} )
			.end( function() {
				done();
			} )
			.error( function( err ) {
				console.log( err );
			} );

	} );

	describe( "When executing within a TransactionContext", function() {
		describe( "and committing the transaction", function() {
			var id;
			var context;
			var insError;
			var insResult;
			var resultsCheck;
			var checkError;
			var readCheck;
			before( function( done ) {
				id = getRowId();
				readCheck = function( done ) {
					sql.execute( config, {
						preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1",
						params: {
							i1: {
								val: id,
								type: sql.INT
							}
						}
					} ).then( function( res ) {
						resultsCheck = res;
						done();
					}, function( err ) {
							checkError = err;
							done();
						} );
				};
				context = sql
					.getTransactionContext( config )
					.step( "insert", {
						preparedSql: "insert into tds_node_test..NodeTestTable (v1, i1) values (@v1, @i1); select SCOPE_IDENTITY() AS NewId;",
						params: {
							i1: {
								val: id,
								type: sql.INT
							},
							v1: {
								val: "testy",
								type: sql.NVARCHAR
							}
						}
					} )
					.end( function( res ) {
						insResult = res;
						res.transaction
							.commit()
							.then( function() {
								readCheck( done );
							} );
					} )
					.error( function( err ) {
						insError = err;
						done();
					} );
			} );
			it( "should have return inserted row", function() {
				expect( resultsCheck.length ).to.be( 1 );
				expect( checkError ).to.not.be.ok();
			} );
			it( "should have returned the identity of inserted row", function() {
				expect( insResult.sets.insert[ 0 ].NewId ).to.be.ok();
				expect( typeof insResult.sets.insert[ 0 ].NewId ).to.be( "number" );
			} );
		} );
		describe( "and rolling back the transaction", function() {
			var id;
			var context;
			var insError;
			var readCheck;
			var resultsCheck;
			var checkError;
			before( function( done ) {
				id = getRowId();
				readCheck = function( done ) {
					sql.execute( config, {
						preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1",
						params: {
							i1: {
								val: id,
								type: sql.INT
							}
						}
					} ).then( function( res ) {
						resultsCheck = res;
						done();
					}, function( err ) {
							checkError = err;
							done();
						} );
				};
				context = sql
					.getTransactionContext( config )
					.step( "insert", {
						preparedSql: "insert into tds_node_test..NodeTestTable (v1, i1) values (@v1, @i1)",
						params: {
							i1: {
								val: id,
								type: sql.INT
							},
							v1: {
								val: "testy",
								type: sql.NVARCHAR
							}
						}
					} )
					.end( function( res ) {
						res.transaction
							.rollback()
							.then( function() {
								readCheck( done );
							} );
					} )
					.error( function( err ) {
						insError = err;
						done();
					} );
			} );
			it( "should show that the row was not inserted", function() {
				expect( resultsCheck.length ).to.be( 0 );
				expect( checkError ).to.not.be.ok();
			} );
		} );
	} );
	describe( "When updating a row", function() {
		var id;
		var insertCheck;
		var insResults;
		var updateCmd;
		var updateErr;
		var updateCheck;
		var updResults;
		before( function( done ) {
			id = getRowId();
			insertCheck = function( done ) {
				sql.execute( config, {
					preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1",
					params: {
						i1: {
							val: id,
							type: sql.INT
						}
					}
				} ).then( function( res ) {
					insResults = res;
					done();
				} );
			};
			updateCheck = function( done ) {
				sql.execute( config, {
					preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1",
					params: {
						i1: {
							val: id,
							type: sql.INT
						}
					}
				} ).then( function( res ) {
					updResults = res;
					done();
				} );
			};
			updateCmd = function( done ) {
				sql.execute( config, {
					preparedSql: "update tds_node_test..NodeTestTable set v1 = @v1 where i1 = @i1",
					params: {
						i1: {
							val: id,
							type: sql.INT
						},
						v1: {
							val: "updatey",
							type: sql.NVARCHAR
						}
					}
				} ).then( function() {
					updateCheck( done );
				}, function( err ) {
						updateErr = err;
					} );
			};
			sql.execute( config, {
				preparedSql: "insert into tds_node_test..NodeTestTable (v1, i1) values (@v1, @i1)",
				params: {
					i1: {
						val: id,
						type: sql.INT
					},
					v1: {
						val: "inserty",
						type: sql.NVARCHAR
					}
				}
			} ).then( function() {
				insertCheck( done );
			} );
		} );

		it( "should have inserted the row", function() {
			expect( insResults.length ).to.be( 1 );
		} );
		it( "should show the updates", function( done ) {
			updateCmd( function() {
				expect( updResults[ 0 ].v1 ).to.be( "updatey" );
				done();
			} );
		} );
	} );
	describe( "When using default connection configuration option", function() {
		it( "Should utilize default options", function( done ) {
			sql.setDefaultConfig( config );
			sql.execute( {
				preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1",
				params: {
					i1: {
						val: getRowId(),
						type: sql.INT
					}
				}
			} ).then( function( /* res */ ) {
				done();
			} );
		} );
	} );

	describe( "When retrieving multiple record sets using preparedSql", function() {
		var id1;
		var id2;
		var insertCheck;
		var insResults;
		var multipleRSCheck;
		var multipleResults;
		before( function( done ) {
			id1 = getRowId();
			id2 = getRowId();
			insertCheck = function( done ) {
				sql.execute( config, {
					preparedSql: "select * from tds_node_test..NodeTestTable where i1 IN ( @i1, @i2 )",
					params: {
						i1: {
							val: id1,
							type: sql.INT
						},
						i2: {
							val: id2,
							type: sql.INT
						}
					}
				} ).then( function( res ) {
					insResults = res;
					done();
				} );
			};
			multipleRSCheck = function( done ) {
				sql.execute( config, {
					preparedSql: "select * from tds_node_test..NodeTestTable where i1 = @i1; select * from tds_node_test..NodeTestTable where i1 = @i2;",
					params: {
						i1: {
							val: id1,
							type: sql.INT
						},
						i2: {
							val: id2,
							type: sql.INT
						}
					},
					multiple: true
				} ).then( function( res ) {
					multipleResults = res;
					done();
				} );
			};

			sql.execute( config, {
				preparedSql: "insert into tds_node_test..NodeTestTable (v1, i1) values (@v1, @i1); insert into tds_node_test..NodeTestTable (v1, i1) values (@v2, @i2); ",
				params: {
					i1: {
						val: id1,
						type: sql.INT
					},
					v1: {
						val: "result1",
						type: sql.NVARCHAR
					},
					i2: {
						val: id2,
						type: sql.INT
					},
					v2: {
						val: "result2",
						type: sql.NVARCHAR
					}
				}
			} ).then( function() {
				insertCheck( done );
			} );
		} );

		it( "should have inserted the rows", function() {
			expect( insResults.length ).to.be( 2 );
		} );
		it( "should return 2 record sets", function( done ) {
			multipleRSCheck( function() {
				expect( multipleResults.length ).to.be( 2 );
				expect( multipleResults[ 0 ].length ).to.be( 1 );
				expect( multipleResults[ 1 ].length ).to.be( 1 );
				expect( multipleResults[ 0 ][ 0 ].v1 ).to.be( "result1" );
				expect( multipleResults[ 1 ][ 0 ].v1 ).to.be( "result2" );
				expect( multipleResults.returnValue ).to.be( 0 );
				done();
			} );
		} );
	} );
	describe( "When retrieving multiple record sets using plain query", function() {
		var id1;
		var id2;
		var insertCheck;
		var insResults;
		var multipleRSCheck;
		var multipleResults;
		before( function( done ) {
			id1 = getRowId();
			id2 = getRowId();
			insertCheck = function( done ) {
				sql.execute( config, {
					query: "select * from tds_node_test..NodeTestTable where i1 IN ( @i1, @i2 )",
					params: {
						i1: {
							val: id1,
							type: sql.INT
						},
						i2: {
							val: id2,
							type: sql.INT
						}
					}
				} ).then( function( res ) {
					insResults = res;
					done();
				} );
			};
			multipleRSCheck = function( done ) {
				sql.execute( config, {
					query: "select * from tds_node_test..NodeTestTable where i1 = @i1; select * from tds_node_test..NodeTestTable where i1 = @i2;",
					params: {
						i1: {
							val: id1,
							type: sql.INT
						},
						i2: {
							val: id2,
							type: sql.INT
						}
					},
					multiple: true
				} ).then( function( res ) {
					multipleResults = res;
					done();
				} );
			};

			sql.execute( config, {
				query: "insert into tds_node_test..NodeTestTable (v1, i1) values (@v1, @i1); insert into tds_node_test..NodeTestTable (v1, i1) values (@v2, @i2); ",
				params: {
					i1: {
						val: id1,
						type: sql.INT
					},
					v1: {
						val: "result1",
						type: sql.NVARCHAR
					},
					i2: {
						val: id2,
						type: sql.INT
					},
					v2: {
						val: "result2",
						type: sql.NVARCHAR
					}
				}
			} ).then( function() {
				insertCheck( done );
			} );
		} );

		it( "should have inserted the rows", function() {
			expect( insResults.length ).to.be( 2 );
		} );
		it( "should return 2 record sets", function( done ) {
			multipleRSCheck( function() {
				expect( multipleResults.length ).to.be( 2 );
				expect( multipleResults[ 0 ].length ).to.be( 1 );
				expect( multipleResults[ 1 ].length ).to.be( 1 );
				expect( multipleResults[ 0 ][ 0 ].v1 ).to.be( "result1" );
				expect( multipleResults[ 1 ][ 0 ].v1 ).to.be( "result2" );
				expect( multipleResults.returnValue ).to.be( undefined );
				done();
			} );
		} );
	} );
} );
