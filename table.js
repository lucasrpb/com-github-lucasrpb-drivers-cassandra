var Types = require('./types');

var TYPE_CODES = Types.TYPE_CODES;

var UDT = require('./udt');
var Index = require('./index');
var Q = require('q');
var Cassandra = require('cassandra-driver');
var Consistency = Cassandra.types.consistencies;
var Clause = require('./clause');
//var Assigment = require('./assignment');
var InsertStm = require('./statements/insert');
var UpdateStm = require('./statements/update');
var DeleteStm = require('./statements/delete');
var BatchStm = require('./statements/batch');
var SelectStm = require('./statements/select');

var Utils = require('com-github-lucasrpb-utils');
require(Utils.StringUtils);
require(Utils.ObjectUtils);

var Table = function(name, schema, db, options){
	this.$name = name;
	this.$db = db;

	this.$client = db.$client;

	this.$fullName = '"'+db.$name+'"."'+name+'"';

	this.$columns = schema.columns;
	this.$partitionKeys = schema.partitionKeys;
	this.$clusterKeys = schema.clusterKeys;
	this.$clusteringOrder = schema.clusteringOrder;
	this.$options = options;

	var indexes = typeof schema.indexes == 'object' ? schema.indexes : [];
	var keys = Object.keys(indexes);

	var $indexes = this.$indexes = [];

	for(var i=0; i<keys.length; ++i){
		var k = keys[i];
		var idx = indexes[k];

		idx.name = '"'+name+'_'+k+'"';

		$indexes.push(idx);
	}

	db.$tables.push(this);
};

Table.create = function(name, schema, db, options){
	return new Table(name, schema, db, options);
};

Table.prototype.create = function(check){
	var $this = this;

	var cql = 'CREATE TABLE';

	if(check){
		cql += ' IF NOT EXISTS';
	}

	cql += ' '+this.$fullName+'(';

	var $columns = this.$columns;
	var i = 0;
	
	for(var col in $columns){
		if($columns.hasOwnProperty(col)){
			var def = $columns[col];

			if(i > 0){
				cql += ',';
			}

			cql += '"'+col+'" '+Types.getType(def.type, this.$db);

			if(def.static){
				cql += ' static';
			}

			i++;
		}
	}

	var pk = this.$partitionKeys.map(function(key){
		return '"'+key+'"';
	});

	cql += ', PRIMARY KEY(';

	var ck = this.$clusterKeys;

	if(ck){
		cql += '('+pk+'), '+ck.map(function(key){
			return '"'+key+'"';
		});
	} else {
		cql += pk;
	}

	cql += '))';
		
	var co = this.$clusteringOrder;

	if(co){
		cql += ' WITH CLUSTERING ORDER BY ('+co.map(function(e){
			var k = Object.keys(e)[0];
			var ord = e[k];

			return '"'+k+'" '+(ord <= 0 ? 'DESC' : 'ASC');
		})+')';
	}
	
	var opts = this.$options;

	if(opts && (opts = opts.$build())){
		if(co){
			cql += ' AND '+opts;
		} else {
			cql += ' WITH '+opts;
		}
	}

	cql += ';';

	return this.$db.$executeDDL(cql);
};

Table.$dropIndex = function(idx, table, check){

	var cql = 'DROP INDEX ';

	if(check){
		cql += ' IF EXISTS';
	}

	cql += ' "'+table.$db.$name+'".'+idx.name+';';

	return table.$db.$executeDDL(cql);
};

Table.$createIndex = function(idx, table, check){

	var $this = this;

	var clazz = idx.class;
	var options = idx.options;

	var cql = 'CREATE';

	if(clazz){
		cql += ' CUSTOM';
	}

	cql += ' INDEX';

	if(check){
		cql += ' IF NOT EXISTS';
	}

	cql += ' '+idx.name+' ON '+table.$fullName+'('+idx.column+')';

	if(clazz){
		cql += " USING '"+clazz+"'";

		var opts = idx.options;

		if(opts){
			cql += ' WITH OPTIONS = {';

			var i = 0;

			for(var name in opts){
				if(opts.hasOwnProperty(name)){
					var value = opts[name];
						
					if(i > 0){
						cql += ',';
					}

					cql += "'"+name+"':"+(typeof value === 'string' ? "'$0'".format(value) : value);

					i++;
				}
			}

			cql += '}';
		}
	}

	cql += ';';
	
	return table.$db.$executeDDL(cql);
};

Table.prototype.drop = function(check){

	var cql = 'DROP TABLE ';

	if(check){
		cql += 'IF EXISTS ';
	}

	cql += this.$fullName+';';

	return this.$db.$executeDDL(cql);
};

Table.prototype.createIndexes = function(check){

	var indexes = this.$indexes;

	if(!indexes.length){
		return Q(true);
	}

	var idx = indexes[0];
	var p = Table.$createIndex(idx, this, check);

	var createPromise = function(idx, check){
		return function(){
			return Table.$createIndex(idx, this, check);
		};
	}

	for(var i=1; i<indexes.length; ++i){
		p = p.then(createPromise(indexes[i], check));
	}

	return p;
};


Table.prototype.dropIndexes = function(check){

	var indexes = this.$indexes;

	if(!indexes.length){
		return Q(true);
	}

	var idx = indexes[0];
	var p = Table.$dropIndex(idx, this, check);

	var dropPromise = function(idx, check){
		return function(){
			return Table.$dropIndex(idx, this, check);
		};
	};

	for(var i=1; i<indexes.length; ++i){
		p = p.then(createPromise(indexes[i], check));
	}

	return p;
};

Table.prototype.createAll = function(check){
	var $this = this;
	return this.create(check).then(function(){
		return $this.createIndexes(check);
	});
};

Table.prototype.insert = function(data){
	return new InsertStm(data, this);
};

Table.prototype.truncate = function(){
	var cql = 'TRUNCATE TABLE '+this.$fullName+';';

	var defer = Q.defer();

	this.$client.execute(cql, {prepare: true}, function(err, result){
		if(err) return defer.reject(err);
		defer.resolve(result);
	});

	return defer.promise;
};

Table.$buildClause = function(clause){

	var cql = '';
	var i = 0;
	var j = 0;
	var params = [];

	for(var c in clause){
		if(clause.hasOwnProperty(c)){

			var ops = clause[c];

			if(i > 0){
				cql += ' AND ';
			}

			j = 0;

			for(var op in ops){
				if(ops.hasOwnProperty(op)){

					var value = ops[op];

					if(j > 0){
						cql += ' AND ';
					}

					//clause += '"'+c+'"';

					cql += c;

					switch(op){
						case Clause.EQ: {
							cql += ' = ';
							break;
						}
						case Clause.LT: {
							cql += ' < ';
							break;
						}
						case Clause.LTE: {
							cql += ' <= ';
							break;
						}
						case Clause.GT: {
							cql += ' > ';
							break;
						}
						case Clause.GTE: {
							cql += ' >= ';
							break;
						}
						case Clause.IN: {
							cql += ' IN ';
							break;
						}
						case Clause.CONTAINS: {
							cql += ' CONTAINS ';
							break;
						}
						case Clause.CONTAINS_KEY: {
							cql += ' CONTAINS KEY ';
							break;
						}
						default: {
							throw new Error('Invalid operator '+op);
						}
					}

					cql += '?';

					params.push(value);

					j++;
				}
			}

			i++;
		}
	}

	return {
		clause: cql,
		params: params
	};
};

Table.prototype.select = function(filter, where){
	return new SelectStm(filter, where, this);
};

Table.prototype.update = function(update, where){
	return new UpdateStm(update, where, this);
};

Table.prototype.delete = function(del, where){
	return new DeleteStm(del, where, this);
};

Table.prototype.batch = function(stms){
	return new BatchStm(stms, this);
};

module.exports = Table;

