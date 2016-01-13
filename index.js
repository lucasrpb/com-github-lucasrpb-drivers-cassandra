module.exports = {
	KEYS: function(column){
		return 'KEYS("'+column+'")';
	},
	ENTRIES: function(column){
		return '"'+column+'"';
	},
};