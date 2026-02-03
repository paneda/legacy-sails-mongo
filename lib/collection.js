/**
 * Module dependencies
 */

var _ = require('@sailshq/lodash');
var async = require('async');
var ObjectId = require('mongodb').ObjectId;
var Errors = require('waterline-errors').adapter;
var utils = require('./utils');
var Document = require('./document');
var Query = require('./query');
const callbackify = require('node:util').callbackify;

/**
 * Manage A Collection
 *
 * @param {Object} definition
 * @api public
 */

var Collection = module.exports = function Collection(definition, connection) {

  // Set an identity for this collection
  this.identity = '';

  // Hold Schema Information
  this.schema = null;

  // Hold a reference to an active connection
  this.connection = connection;

  // Hold the config object
  var connectionConfig = connection.config || {};
  this.config = _.extend({}, connectionConfig.wlNext);

  // Hold Indexes
  this.indexes = [];

  // Parse the definition into collection attributes
  this._parseDefinition(definition);

  // Build an indexes dictionary
  this._buildIndexes();

  return this;
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Find Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.find = function find(criteria, cb) {
  var self = this,
      query;

  // Catch errors from building query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch(err) {
    return cb(err);
  }

  var collection = this.connection.db.collection(self.identity);

  // Check for aggregate query
  if(query.aggregate) {
    var aggregate = [
      { '$match': query.criteria.where || {} },
      { '$group': query.aggregateGroup }
    ];

    const callbackAggregate = callbackify((aggregate, options) => collection.aggregate(aggregate, {}).toArray())
    return callbackAggregate(aggregate, {}, (err, results) => {
      if (err) {
        return cb(err)
      }
      // Results have grouped by values under _id, so we extract them
      var mapped = results.map(function(result) {
        for (var key in result._id) {
          result[key] = result._id[key];
        }
        delete result._id;
        return result;
      });

      cb(null, mapped);
    });
  }

  var where = query.criteria.where || {};
  var queryOptions = _.omit(query.criteria, 'where');

  // Run Normal Query on collection
  // mongo driver 3+ requires only 2 arguments. projection is a new key inside queryOptions
  const callbackFind = callbackify((filter, options) => collection.find(filter, options).toArray())
  callbackFind(where, queryOptions, (err, docs) => {
    if (err) return cb(err);

    cb(null, utils.normalizeResults(docs, self.schema));
  })
};

/**
 * Stream Documents
 *
 * @param {Object} criteria
 * @param {Object} stream
 * @api public
 */
Collection.prototype.stream = function find(criteria, stream) {
  var self = this,
    query;

  // Ignore `select` from waterline core
  if (typeof criteria === 'object') {
    delete criteria.select;
  }

  // Catch errors from building query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch(err) {
    return stream.end(err); // End stream
  }

  var collection = this.connection.db.collection(self.identity);

  var where = query.criteria.where || {};
  var queryOptions = _.omit(query.criteria, 'where');

  // Run Normal Query on collection
  // TODO migrate
  var dbStream = collection.find(where, queryOptions).stream();

  // For each data item
  dbStream.on('data', function(item) {
    // Pause stream
    dbStream.pause();

    var obj = utils.rewriteIds([item], self.schema)[0];

    stream.write(obj, function() {
      dbStream.resume();
    });

  });

  // Handle error, an 'end' event will be emitted after this as well
  dbStream.on('error', function(err) {
    stream.end(err); // End stream
  });

  // all rows have been received
  dbStream.on('close', function() {
    stream.end();
  });
  // stream has ended
  dbStream.on('end', function() {
    stream.end();
  });
};

/**
 * Insert A New Document
 *
 * @param {Object|Array} values
 * @param {Function} callback
 * @api public
 */

Collection.prototype.insert = function insert(values, cb) {

  var self = this;

  // Normalize values to an array
  if(!Array.isArray(values)) values = [values];

  // Build a Document and add the values to a new array
  var docs = values.map(function(value) {
    return new Document(value, self.schema).values;
  });

  const callbackInsert = callbackify((docs) => this.connection.db.collection(this.identity).insertMany(docs))
  callbackInsert(docs, (err, results) => {
    if (err) return cb(err);
    // this now returns only the op details as per this: https://www.mongodb.com/community/forums/t/did-insertone-in-node-js-v4-change-to-not-return-the-inserted-doc/120254
    // {
    //   acknowledged: true,
    //   insertedCount: 1,
    //   insertedIds: {
    //     '0': new ObjectId("65dce11aea5a34fb49dd12ae")
    //   }
    // }

    if (results?.acknowledged === true && _.size(docs) === results.insertedCount) {
      _.each(docs, (doc, index) => {
        if (!results?.insertedIds?.[index]) {
          return cb(`Index ${index} not found to hydrate documents in result ${JSON.stringify(results)}`)
        } else {
          doc._id = results.insertedIds[index]
        }
      })
      cb(null, utils.rewriteIds(docs, self.schema));
    } else {
      cb(`Mismatch in id counts returned in insert docs: ${_.size(docs)}, op result ${JSON.stringify(results)}`)
    }
  })
};

/**
 * Update Documents
 *
 * @param {Object} criteria
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */

Collection.prototype.update = function update(criteria, values, cb) {
  var self = this,
      query;

  // Ignore `select` from waterline core
  if (typeof criteria === 'object') {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch(err) {
    return cb(err);
  }

  values = new Document(values, this.schema).values;

  // Mongo doesn't allow ID's to be updated
  if(values.id) delete values.id;
  if(values._id) delete values._id;

  var collection = this.connection.db.collection(self.identity);

  // Lookup records being updated and grab their ID's
  // Useful for later looking up the record after an insert
  // Required because options may not contain an ID
  const callbackFind = callbackify((query, options) => collection.find(query, options).toArray());
  callbackFind(query.criteria.where, {projection: {_id: 1}}, (err, records) => {
    if(err) return cb(err);
    if(!records) return cb(Errors.NotFound);

    // Build an array of records
    var updatedRecords = [];

    records.forEach(function(record) {
      updatedRecords.push(record._id);
    });

    // Update the records
    const callbackUpdateMany = callbackify((filter, update, options) => collection.updateMany(filter, update, options))
    callbackUpdateMany(query.criteria.where, { '$set': values }, { multi: true }, (err, result) => {
      if(err) return cb(err);

      // Look up newly inserted records to return the results of the update

      callbackFind({ _id: { '$in': updatedRecords }},{}, (err, records) => {
        if(err) return cb(err);
        cb(null, utils.rewriteIds(records, self.schema));
      });
    });
  });
};

/**
 * Destroy Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.destroy = function destroy(criteria, cb) {
  var self = this,
      query;

  // Ignore `select` from waterline core
  if (typeof criteria === 'object') {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch(err) {
    return cb(err);
  }

  var collection = this.connection.db.collection(self.identity);
  const callbackDelete = callbackify((filter) => collection.deleteMany(filter))
  callbackDelete(query.criteria.where, (err, deleteResult) => {
    if(err) return cb(err);

    // Force to array to meet Waterline API
    var resultsArray = [];

    // TODO rethink this approach since it used to send back deleted docs earlier

    // If result is not an array return an array
    // if(!Array.isArray(results)) {
    //   resultsArray.push({ id: results });
    //   return cb(null, resultsArray);
    // }

    // Create a valid array of IDs
    // results.forEach(function(result) {
    //   resultsArray.push({ id: result });
    // });

    cb(null, [deleteResult]);
  });
};

/**
 * Count Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.count = function count(criteria, cb) {

  var self = this;
  var query;

  // Ignore `select` from waterline core
  if (typeof criteria === 'object') {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch(err) {
    return cb(err);
  }

  const callbackCount = callbackify((filter) => this.connection.db.collection(this.identity).countDocuments(filter))
  callbackCount(query.criteria.where, (err, count) => {
    if (err) return cb(err);
    cb(null, count);
  });
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Get name of primary key field for this collection
 *
 * @return {String}
 * @api private
 */
Collection.prototype._getPK = function _getPK () {
  var self = this;
  var pk;

  _.keys(this.schema).forEach(function(key) {
    if(self.schema[key].primaryKey) pk = key;
  });

  if(!pk) pk = 'id';
  return pk;
};


/**
 * Parse Collection Definition
 *
 * @param {Object} definition
 * @api private
 */

Collection.prototype._parseDefinition = function _parseDefinition(definition) {
  var self = this;

  // Hold the Schema
  this.schema = _.cloneDeep(definition.definition);

  if (_.has(this.schema, 'id') && this.schema.id.primaryKey && this.schema.id.type === 'integer') {
    this.schema.id.type = 'objectid';
  }

  // Remove any Auto-Increment Keys, Mongo currently doesn't handle this well without
  // creating additional collection for keeping track of the increment values
  Object.keys(this.schema).forEach(function(key) {
    if(self.schema[key].autoIncrement) delete self.schema[key].autoIncrement;
  });

  // Replace any foreign key value types with ObjectId
  Object.keys(this.schema).forEach(function(key) {
    if(self.schema[key].foreignKey) {
      self.schema[key].type = 'objectid';
    }
  });

  // Set the identity
	var ident = definition.tableName ? definition.tableName : definition.identity.toLowerCase();
	this.identity = _.clone(ident);
};

/**
 * Build Internal Indexes Dictionary based on the current schema.
 *
 * @api private
 */

Collection.prototype._buildIndexes = function _buildIndexes() {
  var self = this;

  Object.keys(this.schema).forEach(function(key) {
    var index = {};
    var options = {};

    // If index key is `id` ignore it because Mongo will automatically handle this
    if(key === 'id') {
      return;
    }

    // Handle Unique Indexes
    if(self.schema[key].unique) {

      // Set the index sort direction, doesn't matter for single key indexes
      index[key] = 1;

      // Set the index options
      options.sparse = true;
      options.unique = true;

      // Store the index in the collection
      self.indexes.push({ index: index, options: options });
      return;
    }

    // Handle non-unique indexes
    if(self.schema[key].index) {

      // Set the index sort direction, doesn't matter for single key indexes
      index[key] = 1;

      // Set the index options
      options.sparse = true;

      // Store the index in the collection
      self.indexes.push({ index: index, options: options });
      return;
    }
  });
};
