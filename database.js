var Utils = require('com-github-lucasrpb-utils');
require(Utils.StringUtils);
require(Utils.ArrayUtils);

var Q = require('q');
var Cassandra = require('cassandra-driver');

var Consistency = Cassandra.types.consistencies;

var Database = function(name, client, logger, options){
	this.$name = name;
	this.$client = client;

	this.$options = options || {class: Database.CLASSES.SIMPLE_STRATEGY, replication_factor: 1};

	this.$udts = []; 
	this.$tables = [];

	this.$logLevel = options.logLevel;
	this.$logger = logger;
};	

var Replication = Database.CLASSES = {
	SIMPLE_STRATEGY: 'SimpleStrategy',
	NETWORK_TOPOLOGY: 'NetworkTopologyStrategy'
};

Database.prototype.$log = function(msg){
	this.$logger.log(this.$logLevel, msg);
};

Database.create = function(name, client, logger, options){
	return new Database(name, client, logger, options);
};

Database.prototype.$executeDDL = function(cql){
	var defer = Q.defer();

	this.$client.execute(cql, {prepare: false, consistency: Consistency.all}, function(err, result) {
	  if(err) return defer.reject(err);
	  defer.resolve(true);
	});

	this.$log(cql+'\n');

	return defer.promise;
};

Database.prototype.create = function(check){
	var $this = this;

	var cql = 'CREATE KEYSPACE';

	if(check){
		cql += ' IF NOT EXISTS';
	}

	cql += ' "'+this.$name+'" WITH REPLICATION = {';

	var $replication = this.$options;

	switch($replication.class){
		case Replication.SIMPLE_STRATEGY: {
			cql += "'class':'"+Replication.SIMPLE_STRATEGY+"', 'replication_factor':"+$replication.replication_factor;
			break;
		}
		case Replication.NETWORK_TOPOLOGY: {

			cql += "'class' : '"+Replication.NETWORK_TOPOLOGY+"',";

			var $datacenters = $replication.datacenters;

			for(var dc in $datacenters){
				if($datacenters.hasOwnProperty(dc)){
					var rfactor = $datacenters[dc];
					cql += "'"+dc+"':"+rfactor;
				}
			}
			
			break;
		}
	}

	cql += '};';

	return this.$executeDDL(cql);
};

Database.prototype.drop = function(check){
	var cql = 'DROP KEYSPACE';

	if(check){
		cql += ' IF EXISTS';
	}

	cql += ' "'+this.$name+'";';

	return this.$executeDDL(cql);
};

Database.prototype.$addUDT = function(udt){
	if(this.$udts.findexOf(function(u){
		return u.$name === udt.$name;
	}) < 0){
		this.$udts.push(udt);
	}
};

Database.prototype.createUDTs = function(check){
	var udts = this.$udts;

	if(!udts.length){
		return Q(true);
	}

	var p = udts[0].create(check);

	var createPromise = function(udt, check){
		return function(){
			return udt.create(check);
		};
	}

	for(var i=1; i<udts.length; ++i){
		p = p.then(createPromise(udts[i], check));
	}

	return p;
};

Database.prototype.dropUDTs = function(check){
	var udts = this.$udts;

	if(!udts.length){
		return Q(true);
	}

	var dropPromise = function(udt, check){
		return function(){
			return udt.drop(check);
		};
	}

	var len = udts.length;

	var p = udts[len-1].drop(check);

	for(var i=len-2; i>=0; --i){
		p = p.then(dropPromise(udts[i], check));
	}

	return p;
};

Database.prototype.createTables = function(check){
	var tables = this.$tables;

	if(!tables.length){
		return Q(true);
	}

	var p = tables[0].createAll(check);

	var createPromise = function(t, check){
		return function(){
			return t.createAll(check);
		};
	}

	for(var i=1; i<tables.length; ++i){
		p = p.then(createPromise(tables[i], check));
	}

	return p;
};

Database.prototype.dropTables = function(check){
	var tables = this.$tables;

	if(!tables.length){
		return Q(true);
	}

	var p = tables[0].drop(check);
		
	var createPromise = function(t, check){
		return function(){
			return t.drop(check);
		};
	};

	for(var i=1; i<tables.length; ++i){
		p = p.then(createPromise(tables[i], check));
	}
	
	return p;
};

Database.prototype.createAll = function(check){
	var $this = this;
	
	return this.create(check).then(function(){
		return $this.createUDTs(check).then(function(){
			return $this.createTables(check);
		});
	});
};

Database.prototype.truncateAll = function(){
	var tables = this.$tables;

	if(!tables.length){
		return Q(true);
	}

	var p = tables[0].truncate();

	var createPromise = function(t){
		return function(){
			return t.truncate();
		};
	};

	for(var i=1; i<tables.length; ++i){
		p = p.then(createPromise(tables[i]));
	}

	return p;
};

module.exports = Database;
