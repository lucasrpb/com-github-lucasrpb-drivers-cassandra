var cassandra = require('./main');
var TYPES = cassandra.TYPES.DATA_TYPES;
var DATA_TYPES = cassandra.TYPES.DATA_TYPES;
var TYPE_CODES = cassandra.TYPES.TYPE_CODES;
var Table = cassandra.Table;
var Database = cassandra.Database;
var UDT = cassandra.UDT;
var index = cassandra.INDEX;
var logger = require('winston');

var Q = require('q');

var Utils = require('com-github-lucasrpb-utils');

require(Utils.ObjectUtils);

var Cassandra = require('cassandra-driver');
var Tuple = Cassandra.types.Tuple;
var Consistency = Cassandra.types.consistencies;

var client = new Cassandra.Client({ 
	contactPoints: ['localhost']
	/*, keyspace: 'demo'*/
});

var TableOptions = cassandra.TableOptions;

var Test = Database.create('test', client, logger, {
	/*class: Database.CLASSES.NETWORK_TOPOLOGY,
	 datacenters: {
	 d1: 2,
	 d3: 3
	 }*/
	logLevel: 'info',
	class: Database.CLASSES.SIMPLE_STRATEGY,
	replication_factor: 1
});

var CepType = UDT.create('Cep', {
	columns: {
		number: {
			type: TYPES.TEXT
		}
	}
}, Test);

var AddressType = UDT.create('Address', {
	columns: {
		street: {
			type: TYPES.TEXT
		},
		cep: {
			type: TYPES.FROZEN(CepType)
		}
	}
}, Test);

var opts = new TableOptions();
var compaction = TableOptions.dateTieredStrategy().baseTimeSeconds(24)
.timestampResolution(TableOptions.TimestampResolution.MILLISECONDS);

opts.comment('testing comment...').compaction(compaction);

var Users = Table.create('Users', {

	columns: {
		username: {
			type: TYPES.TEXT
		},
		age: {
			type: TYPES.INT,
			static: false
		},
		bin: {
			type: TYPES.BLOB
		},
		x: {
			type: TYPES.INT
		},
		tuple: {
			type: TYPES.TUPLE(TYPES.TEXT, TYPES.INT, TYPES.INT)
		},
		dependents: {
			type: TYPES.LIST(TYPES.TEXT)
		},
		addresses: {
			type: TYPES.MAP(TYPES.TEXT, TYPES.FROZEN(AddressType))
		}
	},
	partitionKeys: ['username'],
	clusterKeys: ['age'],
	clusteringOrder: [{age: 0}],
	indexes: {
		'names_idx': {
			column: index.KEYS('addresses'),
			//class: 'X',
			/*options: {
				'p1': "hi"
			}*/
		}
	}
}, Test, opts);

var u1 = {
	username: 'lucasrpb',
	age: 26,
	x: 1,
	bin: new Buffer("my username is lucasrpb", "utf-8"),
	tuple: new Tuple('a', 1, 2),
	dependents: ['Luana', 'Angela', 'Felipe'],
	addresses: {
		'a1': {
			street: 'Av. Brasil',
			cep: {
				number: '2230'
			}
		}
	}
};

var u2 = {
	username: 'luanagpb',
	age: 26,
	x: 2,
	bin: new Buffer("my username is luanagpb", "utf-8"),
	tuple: new Tuple('b', 3, 4),
	dependents: ['Carmen', 'Pamela', 'André'],
	addresses: {
		'a1': {
			street: 'Av. Paraná',
			cep: {
				number: '1250'
			}
		}
	}
};

function generateBatch(){

	return Users.batch([

		// First command
		Users.update(
		// Update
		{
			dependents: {
				$idx: {
					0: 'Daniel',
					1: 'Margarida'
				}
			},
			addresses: {
				$add: {
					'a2': {
						street: 'Av. São Paulo',
						cep: {
							number: '1200'
						}
					},
					'a3': {
						street: 'Av. Beira Rio',
						cep: {
							number: '1000'
						}
					}
				}
			}
		},
		// Where clause
		{
			username: {
				$eq: 'lucasrpb'
			},
			age: {
				$eq: 26
			}
		})
		.ifExists()
		.queryOptions({
			consistency: Consistency.localQuorum
		}),

		// Second command
		Users.delete(
			// Delete
			[{addresses: 'a1'}],

			// Where
			{
				username: {
					$eq: 'lucasrpb'
				},
				age: {
					$eq: 26
				}
			}
		)

	]);

};

Test.drop(true)
.then(function(result){

	console.log('DROPPING KEYSPACE: \n');
	console.log(result);
	console.log('\n');

	return Test.createAll(true);
})
.then(function(result){
	
	console.log('CREATION ALL: \n');
	console.log(result);
	console.log('\n');

	return Users.insert(u1)
				.ifNotExists()
				.options({
					ttl: 20000,
					//timestamp: +new Date()
				})
				.queryOptions({
					consistency: Consistency.localQuorum
				})
				.execute();
})
.then(function(result){

		console.log('INSERTION USER U1: \n');
		console.log(result);
		console.log('\n');

	return Users.insert(u2)
		.ifNotExists()
		.options({
			ttl: 20000,
			//timestamp: +new Date()
		})
		.queryOptions({
			consistency: Consistency.localQuorum
		})
		.execute();
})
.then(function(result){

		console.log('INSERTION USER U2: \n');
		console.log(result);
		console.log('\n');

	return generateBatch().execute();
})
.then(function(result){

	console.log('BATCH COMMAND: \n');
	console.log(result);
	console.log('\n');

	// Default selection (for few rows)
	return Users.select(
		// Filter
		['*'],

		// Where
		{
			username: {
				$eq: 'lucasrpb'
			},
			age: {
				$eq: 26
			}
		}
	)
	.allowFiltering()
	.limit(10)
	.order({
		age: 1
	})
	.execute();
})
.then(function(results){

	console.log('SELECTION: \n');
	console.log(results.rows);
	console.log('\n');

	var defer = Q.defer();

	// Stream selection (the driver parses each row as soon as it is received, yielding rows without buffering them)
	 Users.select(
		// Filter
		['username', '"age" as user_age', 'TTL("x") as x_ttl'],
		// ['"username"'],

		// Where
		{
			age: {
				$lt: 30,
				$gt: 20
			}
		}
	)
	.allowFiltering()
	.stream()
	.on('readable', function () {

		//readable is emitted as soon a row is received and parsed
		var row;
		while (row = this.read()) {
			console.log('row %j\n', row);
		}

	})
	.on('end', function () {

		console.log('no more rows...\n');

		defer.resolve(true);
	})
	.on('error', function (err) {
		defer.reject(err);
	});

	return defer.promise;
})
.then(function(){

	var defer = Q.defer();
	var total = 0;

	// Each: same as the stream, but with callbacks!
	Users.select(
		// Filter
		['*'],

		// Where
		{
			age: {
				$lt: 30,
				$gt: 20
			}
		}
	)
	.allowFiltering()
	.each(
	// Row handler
	function(n, row){

		console.log('row %d! \nbin is: %s\n, %j\n', n, row.bin.toString('utf-8'), row);

		total++;
	},

	// Error handler
	function(err){
		if(err) defer.reject(err);

		console.log('no more results! Total: %d\n', total);
		return defer.resolve(true);
	});

	return defer.promise;
})
.finally(function(){
	client.shutdown();
})
.done();
