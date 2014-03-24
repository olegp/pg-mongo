// based on https://github.com/sergeyksv/tingodb/blob/master/lib/updater.js with lowdash removed

function isPlainObject(value) {
  return (!!value
    && typeof value === 'object'
    && value.constructor === Object);
}

function each(obj, cb) {
  Object.keys(obj).forEach(function(key) {
    cb(obj[key], key);
  });
}

function ensurePath(obj,k,cb) {
  var path = k.split(".")
  var t = null;
  if (path.length==1)
    t = obj
  else {
    var l = obj;
    for (var i=0; i<path.length-1; i++) {
      var p = path[i];
      if (!l[p])
        l[p] = {};
      l = l[p];
    }
    t = l;
    k = path[i];
  }
  cb(t,k)
}

function applySet(obj,$set) {
  each($set, function (v,k) {
    ensurePath(obj,k, function (t,k) {
      // do recursive apply for plain Objects
      if (isPlainObject(v)) {
        if (!t[k])
          t[k]={};
        applySet(t[k],v);
      }
      else
        t[k] = v;
    })
  })
}

function applyUnset(obj,$set) {
  each($set, function (v,k) {
    var path = k.split("."); var t = obj;
    for (var i=0; i<path.length-1 && t[path[i]]; t=t[path[i++]]); k = path[i];
    if (t==null || t[k]==null) return;
    delete t[k];
  })
}

function applyInc(obj, $inc) {
  each($inc, function (v, k) {
    ensurePath(obj,k, function (t,k) {
      if (!t[k]) t[k] = 0;
      if (!isFinite(t[k]))
        throw new Error("Cannot apply $inc modifier to non-number");
      t[k] += v;
    })
  });
}

function applyPush(obj,$push) {
  each($push, function (v,k) {
    ensurePath(obj,k, function (t,k) {
      if (!t[k]) {
        t[k] = v.$each?v.$each:[v];
      } else {
        if (!Array.isArray(t[k]))
          throw new Error("Cannot apply $push/$pushAll modifier to non-array")

        if (v.$each) {
          each(v.$each, function(elem) {
            t[k].push(elem);
          });
        } else
          t[k].push(v);
      }
    })
  })
}

function applyPop(obj,$op) {
  each($op, function (v,k) {
    var path = k.split("."); var t = obj;
    for (var i=0; i<path.length-1 && t[path[i]]; t=t[path[i++]]); k = path[i];
    if (t==null || t[k]==null) return;
    if (Array.isArray(t[k])) {
      if (v>=0)
        t[k]=t[k].slice(0,-1)
      else if (v==-1)
        t[k]=t[k].slice(1)
    } else throw new Error("Cannot apply $pop modifier to non-array");
  })
}

function applyPull(obj,$op,tdb) {
  each($op, function (v,k) {
    var path = k.split("."); var t = obj;
    for (var i=0; i<path.length-1 && t[path[i]]; t=t[path[i++]]); k = path[i];
    if (t==null || t[k]==null) return;
    if (Array.isArray(t[k])) {
      var qt = tdb.Finder.matcher({v:v});
      var matcher = null;
      eval("matcher = function (obj) { return "+ (qt.native()) + " }");
      t[k] = t[k].filter(function (obj) { return !matcher({v:obj}); });
    } else throw new Error("Cannot apply $pull/$pullAll modifier to non-array");
  })
}

function applyPullAll(obj,$op) {
  each($op, function (v,k) {
    var path = k.split("."); var t = obj;
    for (var i=0; i<path.length-1 && t[path[i]]; t=t[path[i++]]); k = path[i];
    if (t==null || t[k]==null) return;
    if (Array.isArray(t[k])) {
      //TODO
      //t[k] = _.without.apply(_,_.union([t[k]],v));
    } else throw new Error("Cannot apply $pull/$pullAll modifier to non-array");
  })
}

function applyRename(obj,$op) {
  each($op, function (v,k) {
    var path = k.split("."); var t = obj;
    for (var i=0; i<path.length-1 && t[path[i]]; t=t[path[i++]]); k = path[i];
    if (t==null || t[k]==null) return;
    ensurePath(obj,v, function (t1,k1) {
      t1[k1] = t[k];
      delete t[k];
    })
  })
}

function applyAddToSet(obj,$op) {
  each($op, function (v,k) {
    ensurePath(obj,k, function (t,k) {
      if (!t[k]) {
        t[k] = v.$each?v.$each:[v];
      } else {
        if (!Array.isArray(t[k]))
          throw new Error("Cannot apply $addToSet modifier to non-array")

        if (v.$each) {
          each(v.$each, function(elem) {
            if (t[k].indexOf(elem)==-1)
              t[k].push(elem);
          });
        } else {
          if (t[k].indexOf(v)==-1)
            t[k].push(v);
        }
      }
    })
  })
}

function applyPushAll(obj,$pushAll) {
  each($pushAll, function (v,k) {
    ensurePath(obj,k, function (t,k) {
      if (!t[k]) {
        t[k] = v;
      } else {
        if (!Array.isArray(t[k]))
          throw new Error("Cannot apply $push/$pushAll modifier to non-array")

        each(v, function(elem) {
          t[k].push(elem);
        });
      }
    })
  })
}

module.exports = function($doc, op, upsert) {
  if (op.$set)
    applySet($doc,op.$set);
  if (op.$unset)
    applyUnset($doc,op.$unset);
  if (op.$inc)
    applyInc($doc, op.$inc);
  if (op.$push)
    applyPush($doc, op.$push);
  if (op.$pushAll)
    applyPushAll($doc, op.$pushAll);
  if (op.$addToSet)
    applyAddToSet($doc, op.$addToSet);
  if (op.$pop)
    applyPop($doc, op.$pop);
  if (op.$pull)
    applyPull($doc, op.$pull, tdb);
  if (op.$pullAll)
    applyPullAll($doc, op.$pullAll);
  if (op.$rename)
    applyRename($doc, op.$rename);
  if (upsert && op.$setOnInsert)
    applySet($doc,op.$setOnInsert);
}

