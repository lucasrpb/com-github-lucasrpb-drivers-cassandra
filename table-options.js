var Utils = require('com-github-lucasrpb-utils');
require(Utils.StringUtils);
require(Utils.NumberUtils);
require(Utils.BooleanUtils);
require(Utils.ArrayUtils);
require(Utils.ObjectUtils);

var CompactionOptions = function(strategy){
	this.$strategy = strategy;

	this.$enabled = null;
	this.$tombstoneCompactionIntervalInDay = null;
	this.$tombstoneThreshold = null;
	this.$uncheckedTombstoneCompaction = null;
	this.$customOptions = [];
};

var CompactionStrategies = CompactionOptions.STRATEGIES = {
	SIZED_TIERED: "'SizeTieredCompactionStrategy'",
	LEVELED: "'LeveledCompactionStrategy'",
	DATE_TIERED: "'DateTieredCompactionStrategy'"
};

CompactionOptions.prototype.enabled = function(enabled){
	if(!Boolean.isBool(enabled)) throw new Error('enabled must be boolean!');
	this.$enabled = enabled;
	return this;
};

CompactionOptions.prototype.tombstoneCompactionIntervalInDay = function(interval){
	if(!Number.isInt(interval)) throw new Error('tombstoneCompactionIntervalInDay must be integer!');
	this.$tombstoneCompactionIntervalInDay = interval;
	return this;
};

CompactionOptions.prototype.tombstoneThreshold = function(threshold){
	if(!Number.isFloat(threshold)) throw new Error('tombstoneThreshold must be float!');
	this.$tombstoneThreshold = threshold;
	return this;
};

CompactionOptions.prototype.uncheckedTombstoneCompaction = function(unchecked){
	if(!Boolean.isBool(unchecked)) throw new Error('uncheckedTombstoneCompaction must be boolean!');
	this.$enabled = enabled;
	return this;
};

CompactionOptions.$freeformOption = function(key, value){
	var is = String.isString(key);
	if(!is || (is && key.length === 0) || key === null || key === undefined){
		throw new Error('Key for custom option should not be null or blank');
	}

	this.$customOptions.push("'$0' : $1".format(key, value.toString()));
};

CompactionOptions.prototype.$buildCommonOptions = function(){
	var options = [];

	options.push("'class' : $0".format(this.$strategy));

	if(this.$enabled !== null){
		options.push("'enabled' : $0".format(this.$enabled));
	}

	if(this.$tombstoneCompactionIntervalInDay !== null){
		options.push("'tombstone_compaction_interval' : $0".format(this.$tombstoneCompactionIntervalInDay));
	}

	if(this.$tombstoneThreshold !== null){
		options.push("'tombstone_threshold' : $0".format(this.$tombstoneThreshold));
	}

	if(this.$uncheckedTombstoneCompaction !== null){
		options.push("'unchecked_tombstone_compaction' : $0".format(this.$uncheckedTombstoneCompaction));
	}

	return options;
};

var SizeTieredCompactionStrategyOptions = function(){
	CompactionOptions.call(this, CompactionStrategies.SIZED_TIERED);

	this.$bucketHigh = null;
	this.$bucketLow = null;
	this.$coldReadsRatioToOmit = null;
	this.$minThreshold = null;
	this.$maxThreshold = null;
	this.$minSSTableSizeInBytes = null;
};

SizeTieredCompactionStrategyOptions.prototype = Object.create(CompactionOptions.prototype);
SizeTieredCompactionStrategyOptions.prototype.constructor = CompactionOptions;

SizeTieredCompactionStrategyOptions.prototype.bucketHigh = function(bucketHigh){
	if(!Number.isFloat(bucketHigh)) throw new Error('bucketHigh must be float!');
	this.$bucketHigh = bucketHigh;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.bucketLow = function(bucketLow){
	if(!Number.isFloat(bucketLow)) throw new Error('bucketLow must be float!');
	this.$bucketLow = bucketLow;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.coldReadsRatioToOmit = function(coldReadsRatio){
	if(!Number.isFloat(coldReadsRatio)) throw new Error('coldReadsRatioToOmit must be float!');
	this.$coldReadsRatioToOmit = coldReadsRatioToOmit;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.minThreshold = function(threshold){
	if(!Number.isInt(threshold)) throw new Error('minThreshold must be integer!');
	this.$minThreshold = threshold;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.maxThreshold = function(threshold){
	if(!Number.isInt(threshold)) throw new Error('maxThreshold must be integer!');
	this.$maxThreshold = threshold;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.minSSTableSizeInBytes = function(size){
	if(!Number.isInt(size)) throw new Error('minSSTableSizeInBytes must be integer!');
	this.$minSSTableSizeInBytes = size;
	return this;
};

SizeTieredCompactionStrategyOptions.prototype.$build = function(){
	var options = [];

	if(this.$bucketHigh !== null){
		options.push("'bucket_high' : $0".format(this.$bucketHigh));
	}
	
	if(this.$bucketLow !== null){
		options.push("'bucket_low' : $0".format(this.$bucketLow));
	}

	if(this.$coldReadsRatioToOmit !== null){
		options.push("'cold_reads_to_omit' : $0".format(this.$coldReadsRatioToOmit));
	}

	if(this.$minThreshold !== null){
		options.push("'min_threshold' : $0".format(this.$minThreshold));
	}

	if(this.$maxThreshold !== null){
		options.push("'max_threshold' : $0".format(this.$maxThreshold));
	}

	if(this.$minSSTableSizeInBytes !== null){
		options.push("'min_sstable_size' : $0".format(this.$minSSTableSizeInBytes));
	}

	return "{$0}".format(options.concat(this.$buildCommonOptions()).toString());
};

var LeveledCompactionStrategyOptions = function(){
	CompactionOptions.call(this, CompactionStrategies.LEVELED);

	this.$ssTableSizeInMB = null;
};

LeveledCompactionStrategyOptions.prototype = Object.create(CompactionOptions.prototype);
LeveledCompactionStrategyOptions.prototype.constructor = LeveledCompactionStrategyOptions;

LeveledCompactionStrategyOptions.prototype.ssTableSizeInMB = function(ssTableSizeInMB){
	if(!Number.isInt(ssTableSizeInMB)) throw new Error('ssTableSizeInMB must be integer!');
	this.$ssTableSizeInMB = ssTableSizeInMB;
	return this;
};

LeveledCompactionStrategyOptions.prototype.$build = function(){
	var options = [];

	if(this.$ssTableSizeInMB !== null){
		options.push("'sstable_size_in_mb' : $0".format(this.$ssTableSizeInMB));
	}
		
	return "{$0}".format(options.concat(this.$buildCommonOptions()).toString());
};

var DateTieredCompactionStrategyOptions = function(){
	CompactionOptions.call(this, CompactionStrategies.DATE_TIERED);

	this.$baseTimeSeconds = null;
	this.$maxSSTableAgeDays = null;
	this.$minThreshold = null;
	this.$maxThreshold = null;
	this.$timestampResolution = null;
};

DateTieredCompactionStrategyOptions.prototype = Object.create(CompactionOptions.prototype);
DateTieredCompactionStrategyOptions.prototype.constructor = DateTieredCompactionStrategyOptions;

DateTieredCompactionStrategyOptions.prototype.baseTimeSeconds = function(baseTimeSeconds){
	if(!Number.isInt(baseTimeSeconds)) throw new Error('baseTimeSeconds must be int!');
	this.$baseTimeSeconds = baseTimeSeconds;
	return this;
};

DateTieredCompactionStrategyOptions.prototype.maxSSTableAgeDays = function(maxSSTableAgeDays){
	if(!Number.isInt(baseTimeSeconds)) throw new Error('maxSSTableAgeDays must be int!');
	this.$maxSSTableAgeDays = maxSSTableAgeDays;
	return this;
};

DateTieredCompactionStrategyOptions.prototype.minThreshold = function(minThreshold){
	if(!Number.isInt(minThreshold)) throw new Error('minThreshold must be int!');
	this.$minThreshold = minThreshold;
	return this;
};

DateTieredCompactionStrategyOptions.prototype.maxThreshold = function(maxThreshold){
	if(!Number.isInt(maxThreshold)) throw new Error('maxThreshold must be int!');
	this.$maxThreshold = maxThreshold;
	return this;
};

var TimestampResolution = DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION = {
	MICROSECONDS: "'MICROSECONDS'",
	MILLISECONDS: "'MILLISECONDS'"
};

DateTieredCompactionStrategyOptions.prototype.timestampResolution = function(timestampResolution){
	this.$timestampResolution = timestampResolution;					
	return this;
};

DateTieredCompactionStrategyOptions.prototype.$build = function(){
	var options = [];

	if(this.$baseTimeSeconds !== null){
		options.push("'base_time_seconds' : $0".format(this.$baseTimeSeconds));
	}

	if(this.$maxSSTableAgeDays !== null){
		options.push("'max_sstable_age_days' : $0".format(this.$maxSSTableAgeDays));
	}

	if(this.$minThreshold !== null){
		options.push("'min_threshold' : $0".format(this.$minThreshold));
	}

	if(this.$maxThreshold !== null){
		options.push("'max_threshold' : $0".format(this.$maxThreshold));
	}	

	if(this.$timestampResolution !== null){
		options.push("'timestamp_resolution' : $0".format(this.$timestampResolution));
	}

	return "{$0}".format(options.concat(this.$buildCommonOptions()).toString());
};

var CompressionOptions = function(algorithm){
	this.$algorithm = algorithm;

	this.$chunkLengthInKb = null;
	this.$crcCheckChance = null;
};

var CompressionAlgorithm = CompressionOptions.ALGORITHM = {
	NONE: "''",
	LZ4: "'LZ4Compressor'",
	SNAPPY: "'SnappyCompressor'",
	DEFLATE: "'DeflateCompressor'"
};

CompressionOptions.prototype.chunkLengthInKb = function(chunkLengthInKb){
	if(!Number.isInt(chunkLengthInKb)) throw new Error('chunkLengthInKb must be int!');
	this.$chunkLengthInKb = chunkLengthInKb;
	return this;
};

CompressionOptions.prototype.crcCheckChance = function(crcCheckChance){
	if(!Number.isInt(crcCheckChance)) throw new Error('crcCheckChance must be float!');
	this.$crcCheckChance = crcCheckChance;
	return this;
};

CompressionOptions.prototype.$build = function(){
	var options = [];

	if(this.$chunkLengthInKb !== null){
		options.push("'chunk_length_kb' : $0".format(this.$chunkLengthInKb));
	}
	
	if(this.$crcCheckChance !== null){
		options.push("'crc_check_chance' : $0".format(this.$crcCheckChance));
	}
			
	return "{$0}".format(options);
};

var TableOptions = function(){
	this.$bloom_filter_fp_chance = null;
	this.$cassandra20Caching = null;
	this.$cassandra21KeyCaching = null;
	this.$cassandra21RowCaching = null;
	this.$commentOption = null;
	this.$compactionOptions = null;
	this.$compressionOptions = null;
	this.$dclocal_read_repair_chance = null;
	this.$default_time_to_live = null;
	this.$gc_grace_seconds = null;
	this.$min_index_interval = null;
	this.$max_index_interval = null;
	this.$index_interval = null;
	this.$memtable_flush_period_in_ms = null;
	this.$populate_io_cache_on_flush = null;
	this.$read_repair_chance = null;
	this.$replicate_on_write = null;
	this.$speculative_retry = null;
};

TableOptions.create = function(){
	return new TableOptions();
};

var Caching = {
	ALL: "'all'",
	KEYS_ONLY: "'keys_only'",
	ROWS_ONLY: "'rows_only'",
	NONE: "'none'"
};

var KeyCaching = {
	ALL: "'all'",
	NONE: "'none'"
};

var CachingRowsPerPartition = {
	NONE: "'none'",
	ALL: "'all'",
	ROWS: function(n){
		if (n <= 0) {
	      throw new Error("rows number for caching should be strictly positive!");
	    }

	    return n.toString();
	}
};

var SpeculativeRetry = {
	PERCENTILE: function(percentile){
		if (percentile < 0 || percentile > 100) {
	      throw new Error("Percentile value for speculative retry should be between 0 and 100");
	    }
	    return '$0 percentile'.format(percentile);
	},
	MILLISECS: function(milisecs){
		if(milisecs < 0){
			throw new Error("Milliseconds value for speculative retry should be positive");
		}
		return '$0 ms'.format(milisecs);
	},
	ALWAYS: "'ALWAYS'",
	NONE: "'NONE'"
};

TableOptions.$validateRateValue = function(value, property){
	if (value < 0 || value > 1.0) {
      throw new Error(property + " should be between 0 and 1");
    }
    return this;
};

TableOptions.prototype.bloomFilterFPChance = function(v){
	if(!Number.isFloat(v)){
		throw new Error('bloomFilterFPChance must be float!');
	}
	TableOptions.$validateRateValue(v, "Bloom filter false positive change");
	this.$bloom_filter_fp_chance = v;
	return this;
};

TableOptions.prototype.compaction = function(v){
	if(!(v instanceof CompactionOptions)){
		throw new Error('compaction must be an instance of CompactionOptions');
	}
	this.$compactionOptions = v;
	return this;
};

TableOptions.prototype.compression = function(v){
	if(!(v instanceof CompressionOptions)){
		throw new Error('compaction must be an instance of CompressionOptions');
	}
	this.$compressionOptions = v;
	return this;
};

TableOptions.prototype.caching = function(v){
	this.$cassandra20Caching = v;
	return this;
};

TableOptions.prototype.keyCaching = function(keys, rows){
	this.$cassandra21KeyCaching = keys;
	this.$cassandra21RowCaching = rows;
	return this;
};

TableOptions.prototype.dcLocalReadRepairChance = function(v){
	if(!Number.isFloat(v)){
		throw new Error('dcLocalReadRepairChance must be float!');
	}
	TableOptions.$validateRateValue(v, "DC local read repair chance");
	this.$dclocal_read_repair_chance = v;
	return this;
};

TableOptions.prototype.defaultTimeToLive = function(v){
	if(!Number.isInt(v)){
		throw new Error('defaultTimeToLive must be int!');
	}
	this.$default_time_to_live = v;
	return this;
};

TableOptions.prototype.gcGraceSeconds = function(v){
	if(!Number.isInt(v)){
		throw new Error('gcGraceSeconds must be int!');
	}
	this.$gc_grace_seconds = v;
	return this;
};

TableOptions.prototype.minIndexInterval = function(v){
	if(!Number.isInt(v)){
		throw new Error('minIndexInterval must be int!');
	}
	this.$min_index_interval = v;
	return this;
};

TableOptions.prototype.maxIndexInterval = function(v){
	if(!Number.isInt(v)){
		throw new Error('maxIndexInterval must be int!');
	}
	this.$max_index_interval = v;
	return this;
};

TableOptions.prototype.index_interval = function(v){
	if(!Number.isInt(v)){
		throw new Error('index_interval must be int!');
	}
	this.$index_interval = v;
	return this;
};

TableOptions.prototype.memtableFlushPeriodInMillis = function(v){
	if(!Number.isInt(v)){
		throw new Error('memtableFlushPeriodInMillis must be int!');
	}
	this.$memtableFlushPeriodInMillis = v;
	return this;
};

TableOptions.prototype.populateIOCacheOnFlush = function(){
	if(!Number.isBool(v)){
		throw new Error('populateIOCacheOnFlush must be boolean!');
	}
	this.$populate_io_cache_on_flush = v;
	return this;
};	

TableOptions.prototype.readRepairChance = function(v){
	if(!Number.isFloat(v)){
		throw new Error('readRepairChance must be float!');
	}
	TableOptions.$validateRateValue(v, "Read repair chance");
	this.$read_repair_chance = v;
	return this;
};

TableOptions.prototype.replicateOnWrite = function(v){
	if(!Number.isBool(v)){
		throw new Error('replicateOnWrite must be boolean!');
	}
	this.$replicate_on_write = v;
	return this;
};

TableOptions.prototype.speculativeRetry = function(v){
	this.$speculative_retry = v;
	return this;
};

TableOptions.prototype.comment = function(v){
	if(!String.isString(v)){
		throw new Error('comment must be a string');
	}
	this.$commentOption = v;

	return this;
};

TableOptions.prototype.$build = function(){
	var osb = [];

	if(this.$bloom_filter_fp_chance !== null){
		osb.push('bloom_filter_fp_chance = $0'.format(this.$bloom_filter_fp_chance));
	}

	if(this.$cassandra20Caching !== null && this.$cassandra21KeyCaching !== null){
		throw new Error('Can\'t use Cassandra 2.0 and 2.1 caching at the same time, you must call only one version of caching()');	
	} else if(this.$cassandra20Caching !== null){
		osb.push('caching = $0'.format(this.$cassandra20Caching));
	} else if(this.$cassandra21KeyCaching !== null && this.$cassandra21RowCaching !== null) {
		osb.push("caching = {'keys' : $0, 'rows_per_partition' : $1}".format(
			this.$cassandra21KeyCaching,
			this.$cassandra21RowCaching
		));
	}

	if(this.$commentOption !== null){
		osb.push("comment = '$0'".format(this.$commentOption));
	}

	if(this.$compactionOptions !== null){
		osb.push('compaction = $0'.format(this.$compactionOptions.$build()));
	}

	if(this.$compressionOptions !== null){
		osb.push('compression = $0'.format(this.$compressionOptions.$build()));
	}

	if(this.$dclocal_read_repair_chance !== null){
		osb.push('dclocal_read_repair_chance = $0'.format(this.$dclocal_read_repair_chance));
	}

	if(this.$default_time_to_live !== null){
		osb.push('default_time_to_live = $0'.format(this.$default_time_to_live));
	}

	if(this.$gc_grace_seconds !== null){
		osb.push('gc_grace_seconds = $0'.format(this.$gc_grace_seconds));
	}

	if(this.$index_interval !== null){
		osb.push('index_interval = $0'.format(this.$index_interval));
	}

	if(this.$min_index_interval !== null){
		osb.push('min_index_interval = $0'.format(this.$min_index_interval));
	}

	if(this.$max_index_interval !== null){
		osb.push('max_index_interval = $0'.format(this.$max_index_interval));
	}

	if(this.$memtable_flush_period_in_ms !== null){
		osb.push('memtable_flush_period_in_ms = $0'.format(this.$memtable_flush_period_in_ms));
	}

	if(this.$populate_io_cache_on_flush !== null){
		osb.push('populate_io_cache_on_flush = $0'.format(this.$populate_io_cache_on_flush));
	}

	if(this.$read_repair_chance !== null){
		osb.push('read_repair_chance = $0'.format(this.$read_repair_chance));
	}

	if(this.$replicate_on_write !== null){
		osb.push('replicate_on_write = $0'.format(this.$replicate_on_write));
	}

	if(this.$speculative_retry !== null){
		osb.push('speculative_retry = $0'.format(this.$speculative_retry));
	}

	return osb.join(' AND ');
};

TableOptions.percentile = function(percentile){
	return SpeculativeRetry.PERCENTILE(percentile);
};

TableOptions.millisecs = function(millisecs){
	return SpeculativeRetry.MILLISECS(millisecs);
};

TableOptions.always = function(){
	return SpeculativeRetry.ALWAYS;
};	

TableOptions.none = function(){
	return SpeculativeRetry.NONE;
};

TableOptions.noRows = function(){
	return CachingRowsPerPartition.NO_ROWS;
};

TableOptions.allRows = function(){
	return CachingRowsPerPartition.ALL_ROWS;
};

TableOptions.rows = function(n){
	return CachingRowsPerPartition.ROWS(n);
};

TableOptions.sizedTieredStrategy = function(){
	return new SizeTieredCompactionStrategyOptions();
};

TableOptions.leveledStrategy = function(){
	return new LeveledCompactionStrategyOptions();
};	

TableOptions.dateTieredStrategy = function(){
	return new DateTieredCompactionStrategyOptions();
};

TableOptions.noCompression = function(){
	return new CompressionOptions(CompressionOptions.ALGORITHM.NONE);
};

TableOptions.lz4 = function(){
	return new CompressionOptions(CompressionOptions.ALGORITHM.LZ4);
};

TableOptions.deflate = function(){
	return new CompressionOptions(CompressionOptions.ALGORITHM.DEFLATE);
};

TableOptions.snappy = function(){
	return new CompressionOptions(CompressionOptions.ALGORITHM.SNAPPY);
};

TableOptions.TimestampResolution = TimestampResolution;

module.exports = TableOptions;

