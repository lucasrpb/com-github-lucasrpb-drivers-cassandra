var Types = require('./types');
var Q = require('q');
var Cassandra = require('cassandra-driver');
var Consistency = Cassandra.types.consistencies;

var Utils = require('com-github-lucasrpb-utils');
require(Utils.StringUtils);

var UDT = function(name, schema, db){
	this.$name = name;
	this.$db = db;

	this.$fullName = '"'+db.$name+'"."'+name+'"';

	this.$columns = schema.columns;
		
	db.$addUDT(this);
};

UDT.create = function(name, schema, db){
	return new UDT(name, schema, db);
};

UDT.prototype.create = function(check){

	var cql = 'CREATE TYPE ';

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

	cql += ');';
							
	return this.$db.$executeDDL(cql);
};

UDT.prototype.drop = function(check){

	var cql = 'DROP TYPE ';

	if(check){
		cql += 'IF EXISTS ';
	}

	cql += this.$fullName+';';

	return this.$db.$executeDDL(cql);
};

module.exports = UDT;