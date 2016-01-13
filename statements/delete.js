var Statement = require('./statement');
var Q = require('q');

var Clause = require('../clause');

var Delete = function(del, where, table){
    Statement.call(this, table);

    this.$where = where;
    this.$delete = del;
    this.$ife = false;
    this.$options = {};
    this.$conditions = {};
};

Delete.prototype = Object.create(Statement.prototype);
Delete.prototype.constructor = Delete;

Delete.prototype.options = function(options){
    this.$options = options;
    return this;
};

Delete.prototype.ifExists = function(){
    this.$ife = true;
    return this;
};

Delete.prototype.conditions = function(conds){
    this.$conditions = conds;
    return this;
};

Delete.prototype.$build = function(){

    var Table = require('../table');

    var del = this.$delete;
    var options = this.$options;
    var where = this.$where;
    var conditions = this.$conditions;

    var cql = 'DELETE';

    var len= del.length;
    var dvalues = [];

    if(len){
        cql += ' ';

        for(var j=0; j<len; ++j){

            if(j > 0){
                cql += ',';
            }

            var e = del[j];

            if(typeof e === 'object'){
                var key = Object.keys(e)[0];

                cql += '"'+key+'"' +'[?]';

                dvalues.push(e[key]);
                continue;
            }

            cql += '"'+e+'"';
        }
    }

    cql += ' FROM '+this.$table.$fullName;

    var keys = Object.keys(options);
    var len= keys.length;

    var ovalues = [];

    if(len){
        cql += ' USING ';

        for(var j=0; j<len; ++j){
            var opt = keys[j];

            if(j > 0){
                cql += ' AND ';
            }

            cql += opt+' ?';

            ovalues.push(options[opt]);
        }
    }

    var ddata = Table.$buildClause(where);

    cql += ' WHERE '+ddata.clause;

    var cvalues = [];

    if(this.$ife){
        cql += ' IF EXISTS';
    } else {

        keys = Object.keys(conditions);
        len = keys.length;

        if(len){

            cql += ' IF ';

            for(var j=0; j<len; ++j){

                if(j > 0){
                    cql += ' AND ';
                }

                var c = keys[j];

                var ocmd = conditions[c];
                var cmd = Object.keys(ocmd)[0];
                var v = ocmd[cmd];

                switch(cmd){
                    case '$eq': {
                        cql += '"'+c+'" = ?';
                        cvalues.push(v);
                        break;
                    }
                    case '$idx': {

                        var k = 0;

                        for(var idx in v){
                            if(v.hasOwnProperty(idx)){
                                if(k > 0){
                                    cql += ' AND ';
                                }

                                cql += '"'+c+'"[?] = ?';
                                cvalues.push(idx, v[idx]);

                                k++;
                            }
                        }

                        break;
                    }
                    default: {
                        throw new Error('Invalid condition command '+cmd);
                    }
                }

            }

        }

    }

    cql += ';';

    return {
        cql: cql,
        params: dvalues.concat(ovalues, ddata.params, cvalues)
    };
};

Delete.prototype.execute = function(){

    var data = this.$build();
    var cql = data.cql;
    var params = data.params;

    this.$table.$db.$log(cql+'\n');

    var defer = Q.defer();

    this.$table.$client.execute(cql, params, Object.merge({prepare: true}, this.$queryOptions), function(err, result){
        if(err) return defer.reject(err);
        defer.resolve(result);
    });

    return defer.promise;
};

module.exports = Delete;