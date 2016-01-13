var Cassandra = require('cassandra-driver');
var Consistency = Cassandra.types.consistencies;

var Utils = require('com-github-lucasrpb-utils');
require(Utils.ObjectUtils);

var Statement = function(table, options){
    this.$table = table;
    this.$queryOptions = {
        consistency: Consistency.localQuorum
    };
};

Statement.prototype.queryOptions = function(options){
    this.$queryOptions = Object.merge(options);
    return this;
};

module.exports = Statement;