Number.isFloat = function(n){
	return !!(n % 1);
};

Number.isInt = function(n){
	return n % 1 === 0;
};