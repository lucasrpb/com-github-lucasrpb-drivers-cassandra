var Statement = require('./statement');
var Q = require('q');
var SelectStm = require('./select');

var Batch = function(stms, table){

    Statement.call(this, table);
    this.$stms = stms;

};

Batch.prototype = Object.create(Statement.prototype);
Batch.prototype.constructor = Batch;

Batch.prototype.execute = function(){
    var $this = this;

    var queries = this.$stms.map(function(stm){

        if(stm instanceof SelectStm){
            throw new Error('Select commands are not allowed in batch commands!');
        }

        var data = stm.$build();
        var cql = data.cql;

        $this.$table.$db.$log(cql+'\n');

        return {
            query: cql,
            params: data.params
        }
    });

    var defer = Q.defer();

    this.$table.$client.batch(queries, Object.merge({ prepare: true }, this.$queryOptions), function(err, result) {
        if(err) return defer.reject(err);
        defer.resolve(result);
    });

    return defer.promise;
};

module.exports = Batch;