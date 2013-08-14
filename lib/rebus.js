var path = require('path');
var fs = require('fs');
var async = require('async');
var crypto = require('crypto');
var syncasyncFacade = require('ypatterns').syncasyncFacade;

// Suffix for all published objects.
var suffix = '.json';
// To avoid collision with objects property names, define string that cannot collide.
var nfs = 'yosefsaysthiscannotbepropertyname';

// Rebus instances created for folders.
// Keep them global to have only one instance per process.
process.rebusinstances = process.rebusinstances || {};
// Used cached on process rebus instances only if they bare the same version.
var rebusversion = 4;

// Rebus factory.
// Creates an instance of rebus in specified folder. The instance can be used in completion callback.
// It is possible to start using rebus instance upon factory return (synchronous pattern), but such
// practice is not recommended for serious production code.
// folder - where the shared files are kept.
// options - by default { persistent : false }, but can be set to true to make the watch persistent.
// callback(err) - completion.
// Returns rebus instnace which includes 3 methods:
//      - publish
//      - subscribe
//      - close
module.exports = function (folder, options, callback) {

  if (typeof options === 'function') {
    callback = options;
    options = null;
  }
  callback = callback || function () { }
  options = options || { persistent: true, singletons: true }
  if (typeof callback !== 'function') {
    throw new Error('invalid callback');
  }
  if (!folder) {
    throw new Error('folder is not specified');
  }

  // Look if singleton for the folder already created.

  if (options.singletons) {
    var singleton = process.rebusinstances[folder];
    if (singleton) {
      console.log('This process already has rebus instance on folder ' + folder);
      // If not the same vesion, go and create new rebus instance.
      if (singleton.version === rebusversion) {
        // Call completion after return value is available.
        process.nextTick(function () {
          callback();
        });
        return singleton;
      }
      else {
        console.warn('Incompatible rebus modules are running in this process. Found rebus instance for ' + folder + ' with incompatible version ' + singleton.version + ' versus current version ' + rebusversion);
      }
    }
  }

  /*
  // Private members.
  */

  // Folder watching handler.
  var watcher;
  // The shared object is built from all published objects.
  var shared = {};
  // Notification handling metadata keeps registered notification callbacks.
  // The layout of metadata matches the layout of shared state (though it does not
  // fully overlap the shared object tree).
  var meta = {};
  // Next free id for notification binding.
  var freeId = 0;
  // Flag used to close rebus instance only once.
  var closed = false;
  // The state of the loder. Keeps files that are not loaded successfully.
  var loader = { errors: {} };
  // Files hashes
  var hashes = [];

  // Create facade for rebus factory, which creates and initializes rebus instance.
  var instance = syncasyncFacade({ create: createInstance, initializeAsync: initializeAsync, initializeSync: initializeSync }, callback);
  // Version can be set immediately, not via facade.
  instance.__defineGetter__("version", function () { return rebusversion; });

  /*
  // Factory methods.
  */

  function createInstance() {
    var instance = { publish: publish, subscribe: subscribe, close: close };
    instance.__defineGetter__("value", function () { return shared; });
    return instance;
  }

  function initializeAsync(instance, callback) {
    // Upon start load all data and create full shared object.
    fs.readdir(folder, function (err, files) {
      if (err) {
        console.error('Failed to read folder ' + folder + ' err:', err);
        callback(err);
        return;
      }

      async.forEach(files, _loadFile, function () {
        _startWatchdog();
        var countErrors = Object.keys(loader.errors).length;
        if (countErrors > 0) {
          // Several files failed to load. As they might be updated by other process, wait for change events
          // until all files are loaded.
          console.warn('Loading ' + countErrors + ' files was not smooth, waiting for updates');
          // Store callback to call once all files are loaded.
          loader.callback = callback;
          return;
        }
        // No errors during loading, all files are loaded.
        loader = null;
        _updateSingleton();
        // Asynchronous initialization is completed.
        callback();
      });
    });
  }

  function initializeSync() {
    var files = fs.readdirSync(folder);
    files.forEach(_loadFileSync);
    _startWatchdog();
    _updateSingleton();
  }

  /*
  // Public rebus instance methods.
  */

  // Publish object.
  // prop - dotted property path.
  // obj - JSON object to publish.
  // callback(err) - completion.
  function publish(prop, obj, callback) {
    callback = callback || function () { };
    if (typeof callback !== 'function') {
      throw new Error('invalid callback');
    }
    if (!prop || typeof prop !== 'string' || prop.length < 1) {
      throw new Error('invalid property path');
    }
    // Write the object to the separate file.
    var shortname = prop + '.json';
    var fullname = path.join(folder, shortname);
    var data = JSON.stringify(obj);
    if (!_checkNewHash(shortname, data)) {
      // No need to publish is the data is the same.      
      return callback();
    }
    var handler = subscribe(prop, function(obj) {
      var updatedData = JSON.stringify(obj);
      if (updatedData === data) {
        handler.close();
        // Completion when notification about change came back.
        return callback();
      }
    });
    fs.writeFile(fullname, data, function (err) {
      if (err) {
        console.error('Failed to write file ' + fullname + ' err:', err);
        handler.close();
        return callback(err);
      }
    });
  }

  // Subscribe on changes.
  // Note that object considered changed if anything in subtree has changed.
  // Also, if property path is inside object that was updated, the change is notified.s
  // prop - dotted property path specifies object that triggers notification.
  // notification - called if object under property path changed.
  // Return - notification handler. Used to stop notification calls (closing the handler).
  function subscribe(prop, notification) {
    if (!notification || typeof notification !== 'function') {
      throw new Error('invalid notification callback');
    }
    return _traverse(_parseProp(prop), null, notification);
  }

  // Cleanup rebus instance.
  function close() {
    if (watcher && !closed) {
      if (options.persistent) {
        // Close handle only if watcher was created persistent.
        watcher.close();
      }
      else {
        // Stop handling change events.
        watcher.removeAllListeners();
        // Leave watcher on error events that may come from unclosed handle.
        watcher.on('error', function (err) { });
      }
      closed = true;
    }
  }

  /*
  // Notification handler public methods.
  */

  // Close the notificaiton handler.
  function closeNotification() {
    delete this[nfs][this.id];
  }

  /*
  // Private functions.
  */

  // Check if file is differrent. Return the new hash if it is.
  function _checkNewHash(filename, data) {
    var shasum = crypto.createHash('sha1');
    shasum.update(data);
    var hash = shasum.digest('hex');
    if (hash === hashes[filename]) {
      return null;
    }
    return hash;
  }

  // Store the instance of rebus per process to be
  // reused if requried again.
  function _updateSingleton() {
    if (!options.singletons) {
      return;
    }
    if (process.rebusinstances[folder]) {
      // Somebody added instance already.
      return;
    }
    // Save this instance to return the same for the same folder.
    process.rebusinstances[folder] = instance;
  }

  function _parseProp(prop) {
    if (!prop || typeof prop !== 'string' || prop.length < 1) {
      throw new Error('invalid property path');
    }
    return prop.split('.');
  }

  // Start watching directory changes.
  function _startWatchdog() {
    if (!watcher) {
      var watcherOptions = { persistent: !!options.persistent };
      watcher = fs.watch(folder, watcherOptions, function (event, filename) {
        if (event === 'change') {
          // On every change load the changed file. This will trigger notifications for interested
          // subscribers.
          _loadFile(filename);
        }
      });
    }
  }


  // Load object from a file. Update state and call notifications.
  function _loadFile(filename, callback) {
    callback = callback || function () { };
    var filepath = path.join(folder, filename);
    fs.readFile(filepath, function (err, data) {
      if (err) {
        console.error('Failed to read file ' + filepath + ' err:', err);
        callback(err);
        return;
      }
      try {
        _loadData(filename, data.toString());
      }
      catch (e) {
        console.info('Object ' + filename + ' was not yet fully written, exception:', e);
        // There will be another notification of change when the last write to file is completed.
        // Meanwhile leave the previous value.
        if (loader) {
          // Store this error to wait until file will be successfully loaded for the 1st time.
          loader.errors[filename] = e;
        }
        // Don't return error to continue asynchronous loading of other files. Errors are assembled on loader.
        callback();
        return;
      }
      console.log('Loaded ' + filename);
      if (loader) {
        if (loader.errors[filename]) {
          // File that previously failed to load, now is loaded.
          delete loader.errors[filename];
          var countErrors = Object.keys(loader.errors).length;
          if (countErrors === 0) {
            // All errors are fixed. This is the time to complete loading.
            var initcb = loader.callback;
            loader = null;
            _updateSingleton();
            initcb();
          }
        }
      }
      callback();
    });
  }

  function _loadFileSync(filename) {
    var data = fs.readFileSync(path.join(folder, filename));
    // If file is written at the same time, this may raise exception. Since synchronous version is not
    // used in serious deployment scenarios, this is not important.
    _loadData(filename, data.toString());
  }

  function _loadData(filename, data) {
    var hash = _checkNewHash(filename, data);
    if (!hash) {
      // Skip loading if the file did not change.
      return;
    }
    var obj = JSON.parse(data);
    hashes[filename] = hash;
    _loadObject(filename, obj);
  }

  function _loadObject(filename, obj, options) {
    var props = filename.split('.');
    // Don't count suffix (.json).
    props.pop();
    _traverse(props, obj, null, options);
  }

  // Traverse the shared object according to property path.
  // If object is specified, call all affected notifications. Those are the notifications along the property path
  // and in the subtree at the end of the path.
  // props - the path in the shared object.
  // obj - if defined, pin the object at the end of the specified path.
  // notification - if defined, pin the notification at the end of the specified path.
  // Returns - if called with notification, returns the handler with information where the notification was pinned, so can be
  // unpinned later.
  function _traverse(props, obj, notification) {
    var length = props.length;
    var refobj = shared;
    var refmeta = meta;
    var handler = {};

    var fns = [];

    for (var i = 0; i < length; i++) {

      var prop = props[i];

      if (!refmeta[prop]) {
        refmeta[prop] = {};
        refmeta[prop][nfs] = {};
      }
      var currentmeta = refmeta[prop];

      if (!refobj[prop]) {
        refobj[prop] = {};
      }
      var currentobj = refobj[prop];

      if (i === (length - 1)) {
        // The end of the path.
        if (obj) {
          // Pin the object here.
          refobj[prop] = obj;
          // Since object changed, append all notifications in the subtree.
          _traverseSubtree(currentmeta, obj, fns);
        }
        if (notification) {
          // Pin notification at the end of the path.
          var id = freeId++;
          currentmeta[nfs][id] = notification;
          // Return value indicates where the notification was pinned.
          handler = { id: id, close: closeNotification };
          handler[nfs] = currentmeta[nfs];
          // Call the notification with initial value of the object.
          // Call notification in the next tick, so that return value from subsribtion
          // will be available.
          process.nextTick(function () {
            notification(currentobj);
          });
        }
      }
      else if (obj) {
        // If change occured, call all notifications along the path.
        _pushNotifications(currentmeta, currentobj, fns);
      }

      // Go deep into the tree.
      refobj = currentobj;
      refmeta = currentmeta;
    }

    if (obj) {
      // Call all notifications.
      async.parallel(fns);
    }

    return handler;
  }

  // Append notificaitons for entire subtree.
  function _traverseSubtree(meta, obj, fns) {
    _pushNotifications(meta, obj, fns);
    for (var key in meta) {
      if (key === nfs) {
        continue;
      }
      var subobj;
      if (obj) {
        subobj = obj[key];
      }
      _traverseSubtree(meta[key], subobj, fns);
    }
  }

  // Append notification from the tree node.
  function _pushNotifications(meta, obj, fns) {
    for (var id in meta[nfs]) {
      fns.push(function (i) {
        return function () {
          meta[nfs][i](obj);
        }
      } (id));
    }
  }

  return instance;
}
