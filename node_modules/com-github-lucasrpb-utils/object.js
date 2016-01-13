/**
 * Merges a configuration object with a default configuration
 */
Object.merge = function(source, def, imutable){
	source = source || {};
	imutable = typeof imutable === 'boolean' ? imutable : false;
	var dest = imutable ? {} : source;
		
	for(var p in def){
		if(def.hasOwnProperty(p)){
			var dvalue = def[p];
			var svalue = source[p];

			if(typeof dvalue === 'object'){
				svalue = svalue || {};
				Object.merge(svalue, def[p]);
				dest[p] = svalue;
				continue;
			}
			
			if(imutable || svalue === undefined || svalue === null){
				dest[p] = dvalue;
			} 
		}
	}

	return dest;		
};
