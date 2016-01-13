var Statement = require('./statement');
var Q = require('q');

var Insert = function(data, table){

    Statement.call(this, table);

    this.$data = data;
    this.$ifne = false;
    this.$options = {};
};

Insert.prototype = Object.create(Statement.prototype);
Insert.prototype.constructor = Insert;

Insert.prototype.ifNotExists = function(){
    this.$ifne = true;
    return this;
};

Insert.prototype.options = function(options){
    this.$options = options;
    return this;
};

Insert.prototype.$build = function(){
    var cql = 'INSERT INTO '+this.$table.$fullName+'(';

    var columns = this.$table.$columns;
    var placeholders = '';
    var values = [];
    var i = 0;
    var data = this.$data;
    var options = this.$options;

    for(var c in data){
        if(columns.hasOwnProperty(c)){

            var def = columns[c];

            // Ignoring non-existing columns...
            if(!def){
                continue;
            }

            if(i > 0){
                cql += ',';
                placeholders += ',';
            }

            cql += '"'+c+'"';
            placeholders += '?';
            values.push(data[c]);

            i++;
        }
    }

    cql += ') VALUES('+placeholders+')';

    if(this.$ifne){
        cql += ' IF NOT EXISTS';
    }

    var keys = Object.keys(options);
    var len = keys.length;

    if(len){
        cql += ' USING ';

        for(var j=0; j<len; ++j){
            var opt = keys[j];

            if(j > 0){
                cql += ' AND ';
            }

            cql += opt+' ?';

            values.push(options[opt]);
        }
    }

    cql += ';';

    return {
      cql: cql,
      params: values
    };
};

Insert.prototype.execute = function(){

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

module.exports = Insert;