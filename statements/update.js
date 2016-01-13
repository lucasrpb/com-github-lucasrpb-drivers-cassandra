var Statement = require('./statement');
var Q = require('q');

var Clause = require('../clause');
var Assigment = require('../assignment');

var Update = function(update, where, table){

    Statement.call(this, table);

    this.$update = update;
    this.$where = where;
    this.$conditions = {};
    this.$ife = false;

    this.$options = {};
};

Update.prototype = Object.create(Statement.prototype);
Update.prototype.constructor = Update;

Update.prototype.options = function(options){
    this.$options = options;
    return this;
};

Update.prototype.conditions = function(conds){
    this.$conditions = conds;
    return this;
};

Update.prototype.ifExists = function(){
    this.$ife = true;
    return this;
};

Update.prototype.$build = function(){

    var Table = require('../table');

    var options = this.$options;
    var where = this.$where;
    var update = this.$update;
    var conditions = this.$conditions;

    var cql = 'UPDATE '+this.$table.$fullName;
    var ovalues = [];

    var i = 0;

    var keys = Object.keys(options);
    var len= keys.length;

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

    i = 0;

    cql += ' SET ';

    var udata = Table.$buildClause(where);

    var params = [];

    for(var c in update){
        if(update.hasOwnProperty(c)){

            if(i > 0){
                cql += ',';
            }

            var ocmd = update[c];
            var cmd = Object.keys(ocmd)[0];
            var v = ocmd[cmd];

            switch (cmd){
                case Assigment.SET: {
                    cql += '"'+c+'" = ?';
                    params.push(v);
                    break;
                }
                case Assigment.ADD: {
                    cql += '"'+c+'" = "'+c+'" + ?';
                    params.push(v);
                    break;
                }
                case Assigment.REM: {
                    cql += '"'+c+'" = "'+c+'" - ?';
                    params.push(v);
                    break;
                }
                case Assigment.INC: {
                    cql += '"'+c+'" = "'+c+'" + ?';
                    params.push(v);
                    break;
                }
                case Assigment.DEC: {
                    cql += '"'+c+'" = "'+c+'" - ?';
                    params.push(v);
                    break;
                }
                case Assigment.IDX: {

                    var keys = Object.keys(v);

                    for(var k=0; k<keys.length; ++k){

                        if(k > 0){
                            cql += ',';
                        }

                        var key = keys[k];
                        var val = v[key];

                        cql += '"'+c+'"[?] = ?';
                        params.push(key, val);

                    }

                    break;
                }
                default: {
                    throw new Error('Invalid UPDATE command '+cmd);
                }
            }

            i++;
        }
    }

    cql += ' WHERE '+udata.clause;

    var $params = ovalues.concat(params, udata.params);

    keys = Object.keys(conditions);
    len = keys.length;

    if(len){
        cql += ' IF ';

        for(var k=0; k<len; ++k){

            if(k > 0){
                cql += ' AND ';
            }

            var cond = keys[k];
            cql += '"'+cond+'" = ?';
            $params.push(conditions[cond]);
        }
    }

    if(this.$ife){
        cql += ' IF EXISTS';
    }

    cql += ';';

    return {
        cql: cql,
        params: $params
    };
};

Update.prototype.execute = function(){

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

module.exports = Update;