var assert = require("assert");
var test = require('test');
var Server = require("../lib/pg-mongo").Server;

var db = new Server('localhost').db("postgres");
db.auth('postgres', 'postgres');

var collection = db.getCollection("tests");

exports.testGetCollection = function() {
  assert(collection);
};

exports.testCollectionNames = function() {
  collection.insert({});
  assert.notEqual(db.collectionNames().indexOf("tests"), -1);
};

exports.testCollectionCount = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert(collection.count(), 2);
};

exports.testDistinct = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  assert.equal(collection.distinct("name", {})[0], "John");
};

exports.testDrop = function() {
  collection.insert({});
  collection.drop();
  assert.equal(db.collectionNames().indexOf("tests"), -1);
  collection = db.getCollection("tests");
};

exports.testInsert = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.find({}).next().test, "test");
};

exports.testSave = function() {
  collection.remove();
  var test = collection.save({test:"test"});
  assert.equal(collection.count(), 1);
  test.test = "test2";
  collection.save(test);
  assert.equal(collection.count(), 1);
  assert.equal(collection.find({}).next().test, "test2");
};

exports.testUpdate = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.count(), 1);
  collection.update({}, {$set:{test:"test2"}});
  assert.equal(collection.count(), 1);
  assert.equal(collection.find({}).next().test, "test2");
};

exports.testEnsureIndex = function() {
  var indexed = db.getCollection("indexed");
  indexed.ensureIndex({name:1});
};

exports.testFind = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.equal(collection.find({result:2}).next().expression, "1 + 1");
  assert.equal(collection.find({result:2}, {expression:1}).next().expression, "1 + 1");
  assert.equal(collection.find({result:2}, {result:1}).next().expression, undefined);
};

exports.testFindOne = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.equal(collection.findOne({result:2}).expression, "1 + 1");
  assert.equal(collection.findOne({result:2}, {expression:1}).expression, "1 + 1");
  assert.equal(collection.findOne({result:2}, {result:1}).expression, undefined);
};

exports.testFindById = function() {
  collection.remove();
  var id = collection.insert({name:"John"})._id;
  assert.equal(collection.findOne({_id:id}).name, "John");
};

exports.testFindNested = function() {
  collection.remove();
  collection.insert({user: {name: "John"}});

  assert.equal(collection.findOne({"user.name":"John"}).user.name, "John");
};

exports.testFindArray = function() {
  collection.remove();
  collection.insert({users: ["Jack", "John"]});

  assert.equal(collection.findOne({"users":"John"}).users.length, 2);
};

exports.testFindAndModify = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}}
  }).test, "test");
  assert.equal(collection.find({}).next().test, "test2");
};

exports.testGetIndexes = function() {
  collection.insert({test:"test"});
  assert.equal(collection.getIndexes()[0].key._id, 1);
};

exports.testRemove = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});

  assert.equal(collection.count(), 4);

  collection.remove({name:"Smith"});
  assert.equal(collection.count(), 3);

  collection.remove({name:"John"}, true);
  assert.equal(collection.count(), 2);

  collection.remove({name:"John"});
  assert.equal(collection.count(), 0);
};

exports.testToArray = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).toArray();
  assert.equal(array.length, 3);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
  assert.equal(array[2].name, "Adam");
};

exports.testForEach = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = [];
  collection.find({}).forEach(function(item) {
    array.push(item);
  });
  assert.equal(array.length, 3);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
  assert.equal(array[2].name, "Adam");
};

exports.testSort = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).sort({name:1}).toArray();
  assert.equal(array[0].name, "Adam");
  assert.equal(array[1].name, "John");
  assert.equal(array[2].name, "Smith");

  array = collection.find({}).sort({name:-1}).toArray();
  assert.equal(array[0].name, "Smith");
  assert.equal(array[1].name, "John");
  assert.equal(array[2].name, "Adam");
};

exports.testSortNested = function() {
  collection.remove();
  collection.insert({data: {name:"John"}});
  collection.insert({data: {name:"Smith"}});
  collection.insert({data: {name:"Adam"}});

  var array = collection.find({}).sort({"data.name":1}).toArray();
  assert.equal(array[0].data.name, "Adam");
  assert.equal(array[1].data.name, "John");
  assert.equal(array[2].data.name, "Smith");
}

exports.testLimit = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).limit(2).toArray();
  assert.equal(array.length, 2);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
};

exports.testSkip = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).skip(1).toArray();
  assert.equal(array.length, 2);
  assert.equal(array[0].name, "Smith");
  assert.equal(array[1].name, "Adam");
};

exports.testCount = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.equal(collection.find({}).count(), 2);
  assert.equal(collection.find({}).skip(1).count(), 2);
  assert.equal(collection.find({}).limit(1).count(), 2);
  assert.equal(collection.find({}).skip(1).limit(1).count(), 2);
};

exports.testSize = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.equal(collection.find({}).size(), 2);
  assert.equal(collection.find({}).skip(1).size(), 1);
  assert.equal(collection.find({}).limit(1).size(), 1);
  assert.equal(collection.find({}).skip(1).limit(1).size(), 1);
};

exports.testMap = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});
  var array = collection.find({}).map(function(user) {
    return user.name;
  }).toArray();
  assert.equal(array[0], "John");
  assert.equal(array[1], "Smith");
  assert.equal(array[2], "Adam");
};

exports.testNext = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var cursor = collection.find({});
  assert.equal(cursor.next().name, "John");
  assert.equal(cursor.next().name, "Smith");
  assert.equal(cursor.next().name, "Adam");
};

if (require.main === module) {
  test.run(exports);
}