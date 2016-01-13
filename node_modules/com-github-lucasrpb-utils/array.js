Array.prototype.findexOf = function(f){

	for(var i=0; i<this.length; ++i){
		var e = this[i];

		if(f(e)){
			return i;
		}
	}
	
	return -1;
};
