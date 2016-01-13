var Statement = require('./statement');
var Q = require('q');

var Select = function(filter, where, table){

    Statement.call(this, table);

    this.$where = where;
    this.$filter = filter;
    this.$limit = 0;
    this.$order = {};
    this.$options = {};
    this.$allowFiltering = false;
    this.$distinct = false;

};

Select.prototype = Object.create(Statement.prototype);
Select.prototype.constructor = Select;

Select.prototype.options = function(opts){
    this.$options = opts;
    return this;
};

Select.prototype.limit = function(limit){
    this.$limit = limit;
    return this;
};

Select.prototype.order = function(order){
    this.$order = order;
    return this;
};

Select.prototype.allowFiltering = function(){
    this.$allowFiltering = true;
    return this;
};

Select.prototype.distinct = function(){
    this.$distinct = true;
    return this;
};

Select.prototype.$generateSelectCQL = function(){
    var Table = require('../table');

    var clause = Table.$buildClause(this.$where);

    var i = 0;

    var cql = 'SELECT';

    if(this.$distinct){
        cql += ' DISTINCT';
    }

    cql += ' '+this.$filter+' FROM '+this.$table.$fullName+' WHERE '+clause.clause;

    var order = this.$order;
    var keys = Object.keys(order);
    var len = keys.length;

    if(len){
        cql += ' ORDER BY ';

        for(var k=0; k<len; ++k){
            var c = keys[k];

            if(k > 0){
                cql += ',';
            }

            cql += '"'+c+'" '+(order[c] ? 'ASC' : 'DESC');
        }
    }

    if(this.$limit){
        cql += ' LIMIT '+this.$limit;
    }

    if(this.$allowFiltering){
        cql += ' ALLOW FILTERING';
    }

    cql += ';';

    return {
        cql: cql,
        params: clause.params
    };
};

Select.prototype.execute = function(){

    var sdata = this.$generateSelectCQL();
    var cql = sdata.cql;

    this.$table.$db.$log(cql+'\n');

    var defer = Q.defer();

    this.$table.$client.execute(cql, sdata.params, Object.merge({prepare: true}, this.$queryOptions), function(err, results){
        if(err) return defer.reject(err);
        defer.resolve(results);
    });

    return defer.promise;
};

Select.prototype.stream = function(){
    var sdata = this.$generateSelectCQL();
    var cql = sdata.cql;

    this.$table.$db.$log(cql+'\n');

    return this.$table.$client.stream(cql, sdata.params, Object.merge({prepare: true}, this.$queryOptions));
};

Select.prototype.each = function(rowHandler, errorHandler){
    var sdata = this.$generateSelectCQL();
    var cql = sdata.cql;

    this.$table.$db.$log(cql+'\n');

    //Reducing a large result
    this.$table.$client.eachRow(cql, sdata.params, Object.merge({prepare: true}, this.$queryOptions), rowHandler, errorHandler);
};

module.exports = Select;