String.prototype.format = function(){
	args = arguments.length === 1 ? 
	(arguments['0'] instanceof Array ? arguments['0'] : 
		(typeof arguments['0'] === 'object' ? arguments['0'] : [arguments['0']])) : arguments; 

	return this.replace(/\$(\w+)/g, function(match, idx){
		return args[idx];
	});
};

String.isString = function(str){
	return typeof str === 'string';
};