var Utils = require('com-github-lucasrpb-utils');
require(Utils.StringUtils);

var TYPE_CODES = {
	ASCII: 1,
	TEXT: 2,
	INT: 3,
	BIGINT: 4,
	BLOB: 5,
	BOOLEAN: 6,
	DECIMAL: 7,
	DOUBLE: 8,
	FLOAT: 9,
	INET: 10,
	TIMESTAMP: 11,
	TIMEUUID: 12,
	TUPLE: 13,
	UUID: 14,
	VARCHAR: 15,
	VARINT: 16,
	COUNTER: 17,
	LIST: 18,
	SET: 19,
	MAP: 20,
	FROZEN: 21
};

var DATA_TYPES = {
	ASCII: {
		code: TYPE_CODES.ASCII
	},
	TEXT: {
		code: TYPE_CODES.TEXT
	},
	INT: {
		code: TYPE_CODES.INT
	},
	BIGINT: {
		code: TYPE_CODES.BIGINT
	},
	BLOB: {
		code: TYPE_CODES.BLOB
	},
	BOOLEAN: {
		code: TYPE_CODES.BOOLEAN
	},
	DECIMAL: {
		code: TYPE_CODES.DECIMAL
	},
	DOUBLE: {
		code: TYPE_CODES.DOUBLE
	},
	FLOAT: {
		code: TYPE_CODES.FLOAT
	},
	INET: {
		code: TYPE_CODES.INET
	},
	TIMESTAMP: {
		code: TYPE_CODES.TIMESTAMP
	},
	TIMEUUID: {
		code: TYPE_CODES.TIMEUUID
	},
	UUID: {
		code: TYPE_CODES.UUID
	},
	VARCHAR: {
		code: TYPE_CODES.VARCHAR
	},
	VARINT: {
		code: TYPE_CODES.VARINT
	},
	COUNTER: {
		code: TYPE_CODES.COUNTER
	},
	TUPLE: function(types){
		return {
			code: TYPE_CODES.TUPLE,
			types: Array.prototype.slice.call(arguments)
		};
	},
	LIST: function(t1){
		return {
			code: TYPE_CODES.LIST,
			t1: t1
		};
	},
	SET: function(t1){
		return {
			code: TYPE_CODES.SET,
			t1: t1
		};
	},
	MAP: function(t1, t2){
		return {
			code: TYPE_CODES.MAP,
			t1: t1,
			t2: t2
		};
	},
	FROZEN: function(t1){
		return {
			code: TYPE_CODES.FROZEN,
			t1: t1
		};
	}
};

module.exports = {
	TYPE_CODES: TYPE_CODES,
	DATA_TYPES: DATA_TYPES,
	getType: function getType(tpe, db){
		var code = tpe.code;

		switch(code){
			case TYPE_CODES.ASCII: {
				return 'ascii';
			}
			case TYPE_CODES.TEXT: {
				return 'text';
			}
			case TYPE_CODES.INT: {
				return 'int';
			}
			case TYPE_CODES.BIGINT: {
				return 'bigint';
			}
			case TYPE_CODES.BLOB: {
				return 'blob';
			}
			case TYPE_CODES.BOOLEAN: {
				return 'boolean';
			}
			case TYPE_CODES.DECIMAL: {
				return 'decimal';
			}
			case TYPE_CODES.DOUBLE: {
				return 'double';
			}
			case TYPE_CODES.FLOAT: {
				return 'float';
			}
			case TYPE_CODES.INET: {
				return 'inet';
			}
			case TYPE_CODES.TIMESTAMP: {
				return 'timestamp';
			}
			case TYPE_CODES.TIMEUUID: {
				return 'timeuuid';
			}
			case TYPE_CODES.UUID: {
				return 'uuid';
			}
			case TYPE_CODES.VARCHAR: {
				return 'varchar';
			}
			case TYPE_CODES.VARINT: {
				return 'varint';
			}
			case TYPE_CODES.COUNTER: {
				return 'counter';
			}
			case TYPE_CODES.TUPLE: {
				var types = tpe.types;
				return 'tuple<'+types.map(function(t){
						return getType(t, db);
					})+'>';
			}
			case TYPE_CODES.LIST: {
				var t1 = tpe.t1;
				return 'list<$0>'.format(getType(t1, db));
			}
			case TYPE_CODES.SET: {
				var t1 = tpe.t1;
				return 'set<$0>'.format(getType(t1, db));
			}
			case TYPE_CODES.MAP: {
				var t1 = tpe.t1;
				var t2 = tpe.t2;

				return 'map<$0, $1>'.format(getType(t1, db), getType(t2, db));
			}
			case TYPE_CODES.FROZEN: {
				var t1 = tpe.t1;
				return 'frozen<$0>'.format(getType(t1, db));
			}
			default: {
				if(typeof tpe === 'string'){
					return '$0'.format('"'+db.$name+'"."'+tpe+'"');
				}

				var UDT = require('./udt');

				if(tpe instanceof UDT){
					return '$0'.format(tpe.$fullName);
				}

				throw new Error('Invalid type $type'.format(code))
			}
		}

	}
};

