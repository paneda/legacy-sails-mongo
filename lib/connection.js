/**
 * Module dependencies
 */

var async = require('async'),
  MongoClient = require('mongodb').MongoClient;
const callbackify = require('node:util').callbackify;
/**
 * Manage a connection to a Mongo Server
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function Connection(config, cb) {
  var self = this;

  // Hold the config object
  this.config = config || {};

  // Build Database connection
  this._buildConnection(function(err, client) {
    if (err) return cb(err);
    if (!client.db()) return cb(new Error('no db object'));

    // Store the DB object
    // mongo driver 3+ returns mongoClient in place of db on _buildConnection.
    // extracting db using client.db() and storing a reference to the client for further use
    self.db = client.db();
    self.client = client;

    // Return the connection
    cb(null, self);
  });
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Create A Collection
 *
 * @param {String} name
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Connection.prototype.createCollection = function createCollection(name, collection, cb) {
  var self = this;

  // Create the Collection
  // this method is promise based in driver 5+
  const callbackCreateCollection = callbackify((name) => this.db.createCollection(name))
  callbackCreateCollection(name, (e, coll) => {
    if (e && (e.code == 48 || e.codeName === "NamespaceExists")) {
      // console.log("ignoring error for existing collection", name)
      self._ensureIndexes(this.db.collection(name), collection.indexes, cb);
    } else if (e) {
      // console.log("Error creating collection", name, e)
      cb(e);
    } else {
      // Create Indexes
      self._ensureIndexes(coll, collection.indexes, cb);
    }
  })
};

/**
 * Drop A Collection
 *
 * @param {String} name
 * @param {Function} callback
 * @api public
 */

Connection.prototype.dropCollection = function dropCollection(name, cb) {
  const callbackDropCollection = callbackify((name) => this.db.dropCollection(name));
  callbackDropCollection(name, (e) => {
    if (e && (e.code == 26 || e.codeName === "NamespaceNotFound")) {
      // console.log("ignoring error for non existent collection", name)
      cb()
    } else if (e) {
      // console.log("Error dropping collection", name, e)
      cb(e);
    } else {
      // console.log("Dropped collection", name)
      cb();
    }
  })
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Build Server and Database Connection Objects
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype._buildConnection = function _buildConnection(cb) {

  // Set the configured options
  var connectionOptions = {};

  if (this.config.mongos)
    connectionOptions = Object.assign(connectionOptions, this.config.mongos);

  if (this.config.replSet)
    connectionOptions = Object.assign(connectionOptions, this.config.replSet);

  // Build up options used for creating a Server instance
  connectionOptions = Object.assign(connectionOptions, {
    readPreference: this.config.readPreference,
    ssl: this.config.ssl,
    sslValidate: this.config.sslValidate,
    sslCA: this.config.sslCA,
    sslCert: this.config.sslCert,
    sslKey: this.config.sslKey,
    // poolSize: this.config.poolSize,
    // autoReconnect: this.config.auto_reconnect,
    // reconnectInterval: this.config.reconnectInterval,
    // reconnectTries: this.config.reconnectTries,
    // useUnifiedTopology: true // disabled the above 3 options since they are incompatible with unified topology
    // useUnifiedTopology also deprecated hence removed
  });

  // Build up options used for creating a Database instance
  connectionOptions = Object.assign(connectionOptions, {
    // w: this.config.w, // deprecated
    // wtimeout: this.config.wtimeout, // deprecated
    readPreference: this.config.readPreference,
    // native_parser: this.config.nativeParser,
    forceServerObjectId: this.config.forceServerObjectId
  });

  // Build A Mongo Connection String
  var connectionString = 'mongodb://';

  // If auth is used, append it to the connection string
  if (this.config.user && this.config.password) {

    // Ensure a database was set if auth in enabled
    if (!this.config.database) {
      throw new Error('The MongoDB Adapter requires a database config option if authentication is used.');
    }

    connectionString += this.config.user + ':' + this.config.password + '@';
  }

  // Append the host and port
  connectionString += this.config.host + ':' + this.config.port + '/';

  if (this.config.database) {
    connectionString += this.config.database;
  }

  // Use config connection string if available
  if (this.config.url) connectionString = this.config.url;

  // Open a Connection
  const callbackConnect = callbackify((connectionString, connectionOptions) => MongoClient.connect(connectionString, connectionOptions))
  callbackConnect(connectionString, connectionOptions, cb)
};

/**
 * Ensure Indexes
 *
 * @param {String} collection
 * @param {Array} indexes
 * @param {Function} callback
 * @api private
 */

Connection.prototype._ensureIndexes = function _ensureIndexes(collection, indexes, cb) {
  var self = this;

  function createIndex(item, next) {
    const callbackCreateIndex = callbackify((index, options) => collection.createIndex(index, options))
    callbackCreateIndex(item.index, item.options, next)
  }

  async.each(indexes, createIndex, cb);
};