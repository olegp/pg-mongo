
var Fiber = global.Fiber || require("fibers");
var pg = require("pg-sync");
var updater = require("./updater");

function Server(server) {
  this._host = server;
}

exports.Server = Server;

Server.prototype.db = function(name) {
  return new DB(this, name);
};

Server.prototype.close = function() {
  pg.end();
};

function DB(server, name) {
  this._server = server;
  this._name = name;
}

exports.DB = DB;

DB.prototype.getMongo = function() {
  return this._server;
};

DB.prototype.getName = function() {
  return this._name;
};

DB.prototype.getCollection = function(name) {
  return new Collection(this, name);
};

DB.prototype.collectionNames = function() {
  return this._client.query("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE'").rows.map(function(row) {
    return row.table_name;
  });
};

DB.prototype.addUser = function() {

};

DB.prototype.dropDatabase = function() {
  // NOTE these need you to be connected to e.g. "postgres" database
  // SELECT datname FROM pg_database WHERE datistemplate = false; to list all databases
  // this._client.query('create database ' + this._name);
  // this._client.query('drop database ' + this._name);
};

DB.prototype.eval = function() {

};

DB.prototype.removeUser = function() {

};

DB.prototype.runCommand = function() {

};

DB.prototype.auth = function(user, pass) {
  this._user = user;
  this._pass = pass;
  this._client = new pg.Client('postgres://' + this._user + ':' + this._pass
    + '@' + this._server._host + '/' + this._name);
};

DB.prototype.getLastErrorObj = function() {

};

function uuid() {
  function pad(s) {
    if (s.length < 4) {
      s = ('0000' + s).slice(-4);
    }
    return s;
  }

  function S4() {
    return pad(Math.floor(Math.random() * 0x10000).toString(16));
  }

  var r = [];
  for (var i = 0; i < 8; i++) {

    r.push(S4());
  }

  return r.join('');
}

function Collection(db, name) {
  this._db = db;
  this._name = name;
  db._client.query('create table if not exists ' + name + ' (_id uuid primary key, data json)');
}

exports.Collection = Collection;

Collection.prototype.getDB = function() {
  return this._db;
};


Collection.prototype.count = function() {
  return +this._db._client.query('select count(*) from ' + this._name).rows[0].count;
};

Collection.prototype.distinct = function(field, criteria) {
  var params = [field.split('.')];
  var query = "select distinct data#>>$1 as value from " + this._name;
  var where = getWhere(criteria, params);
  if(where) {
    query += ' where ' + where.join(' and ');
  }
  return this._db._client.query(query, params).rows.map(function(row) {
    return row.value;
  });
};

Collection.prototype.dropIndex = function() {

};

Collection.prototype.ensureIndex = function(keys, options) {
  var indices = [];
  Object.keys(keys).forEach(function(key) {
    var attributes = key.split('.').map(function(k) {
      return "->'" + k + "'";
    });
    attributes.push(attributes.pop().replace('>', '>>'));
    indices.push("(data" + attributes.join('') + ")");
  });
  //TODO name index, do not create one if it already exists
  var query = "create index on " + this._name + "(" + indices.join(', ') + ")";
  this._db._client.query(query);
};

Collection.prototype.getIndexes = function() {

};

Collection.prototype.findOne = function(criteria, projection) {
  return this.find(criteria, projection).limit(1).next();
};

Collection.prototype.insert = function(document) {
  var id = uuid();
  this._db._client.query('insert into ' + this._name + ' values ($1, $2)', id, JSON.stringify(document));
  document._id = id;
  return document;
};

Collection.prototype.save = function(document) {
  if(document._id) {
    return this._update(document);
  } else {
    return this.insert(document);
  }
};

Collection.prototype.remove = function(query, justOne) {
  var params = [];
  var where = getWhere(query, params);
  var query = 'delete from ' + this._name;
  if(where) {
    query += ' where ' + where.join(' and ');
  }
  if(justOne) {
    if(!where) {
      query += ' where ';
    } else {
      query += ' and ';
    }
    query += '_id in (select _id from ' + this._name + ' limit 1)';
  }
  this._db._client.query(query, params);
};

Collection.prototype.findAndModify = function(options) {
  var document = this.findOne(options.query);
  var r = JSON.parse(JSON.stringify(document));
  updater(document, options.update, options.upsert);
  this._update(document);
  //TODO sort, remove, new, fields, upsert
  return r;
};

Collection.prototype._update = function(document) {
  var id = document._id;
  delete document._id;
  this._db._client.query('update ' + this._name + ' set data=$1 where _id=$2', JSON.stringify(document), id);
  document._id = id;
  return document;
};

Collection.prototype.update = function(query, update, options) {
  if(options && options.multi) {
    this.find(query).toArray().map(function(document) {
       updater(document, update);
       this._update(document);
    });
  } else {
    //TODO upsert
    var document = this.findOne(query);
    if(document) {
      updater(document, update, options && options.upsert);
      this._update(document);
    }
  }
};

function getFields(projection, params) {
  if(projection) {
    var keys =  Object.keys(projection);
    if(keys.length) {
      var fields = [];
      var counter = params.length + 1;
      keys.forEach(function(key) {
        fields.push("data#>>$" + (counter ++) + " as " + key);
        params.push(key.split('.'));
      });
      return fields.join(', ');
    }
  }
  return '*';
}

function getOrder(sort, params) {
  var keys =  Object.keys(sort);
  var fields = [];
  var counter = params.length + 1;
  keys.forEach(function(key) {
    fields.push("data#>>$" + (counter ++) + " " +  (sort[key] > 0 ? "asc" : "desc"));
    params.push(key.split('.'));
  });
  return fields.join(', ');
}

function getWhere(criteria, params) {
  if(criteria) {
    var keys =  Object.keys(criteria);
    if(keys.length) {
      var counter = params.length + 1;
      var where = [];
      keys.forEach(function(key) {
        if(key == '_id') {
          where.push("_id=$" + (counter ++));
        } else {
          where.push("data#>>$" + (counter ++) + " = $" + (counter ++));
          params.push(key.split('.'));
        }
        params.push(criteria[key]);
      });
      return where;
    }
  }
}

Collection.prototype.find = function(criteria, projection) {
  this._criteria = criteria;
  this._projection = projection;
  return new Cursor(this);
};

Collection.prototype.drop = function() {
  this._db._client.query('drop table ' + this._name);
};

function Cursor(collection) {
  this._collection = collection;
  this._mapper = function(value) {
    return value;
  };
}

Cursor.prototype.limit = function(limit) {
  this._limit = limit;
  return this;
};

Cursor.prototype.skip = function(skip) {
  this._skip = skip;
  return this;
};

Cursor.prototype.sort = function(sort) {
  this._sort = sort;
  return this;
};

Cursor.prototype.count = function() {
  return this._collection.count();
};

Cursor.prototype.size = function() {
  return this.toArray().length;
};

Cursor.prototype.forEach = function(fn) {
  this.toArray().forEach(fn);
  return this;
};

Cursor.prototype.map = function(mapper) {
  this._mapper = mapper;
  return this;
};

function merge() {
  var result = {};
  for (var i = arguments.length; i > 0; --i) {
    var obj = arguments[i - 1];
    for (var property in obj) {
      result[property] = obj[property];
    }
  }
  return result;
}

function rowToDocument(row) {
  var data = row.data;
  delete row.data;
  return merge(row, data);
}

Cursor.prototype.next = function() {
  if(!this._data) {
    this._data = this.toArray();
  }
  return this._data.shift();
};

Cursor.prototype.toArray = function() {
  var params = [];
  var fields = getFields(this._collection._projection, params);
  var where = getWhere(this._collection._criteria, params);
  var query = 'select ' + fields + ' from ' + this._collection._name;
  if(where) {
    query += ' where ' + where.join(' and ');
  }
  if(this._sort) {
    query += " order by " + getOrder(this._sort, params);
  }
  if(this._limit) {
    query += " limit " + +this._limit;
  }
  if(this._skip) {
    query += " offset " + +this._skip;
  }
  return this._collection._db._client.query(query, params).rows.map(rowToDocument).map(this._mapper);
};