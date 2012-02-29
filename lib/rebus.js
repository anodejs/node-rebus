var path = require('path');
var fs = require('fs');
var async = require('async');
var syncasyncFacade = require('ypatterns').syncasyncFacade;

// Suffix for all published objects.
var suffix = '.json';
// To avoid collision with objects property names, define string that cannot collide.
var nfs = 'yosefsaysthiscannotbepropertyname';

// Rebus instances created for folders.
var singletons = {};

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
    options = options || { persistent: false, singletons: true }
    if (typeof callback !== 'function') {
        throw new Error('invalid callback');
    }
    if (!folder) {
        throw new Error('folder is not specified');
    }

    // Look if singleton for the folder already created.

    if (options.singletons) {
        var singleton = singletons[folder];
        if (singleton) {
            // Call completion after return value is available.
            process.nextTick(function () {
                callback();
            });
            return singleton;
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

    // Create facade for rebus factory, which creates and initializes rebus instance.
    var instance = syncasyncFacade({ create: createInstance, initializeAsync: initializeAsync, initializeSync: initializeSync }, callback); ;
    if (options.singletons) {
        // Save this instance to return the same for the same folder.
        singletons[folder] = instance;
    }

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

            async.forEach(files, _loadFile, function (err) {
                if (err) {
                    console.warn('Loading all files was not smooth, err:', err);
                }
                // Regardless errors, start watching the directory.
                _startWatchdog();
                // Asynchronous initialization is completed.
                callback();
            });
        });
    }

    function initializeSync() {
        var files = fs.readdirSync(folder);
        files.forEach(_loadFileSync);
        _startWatchdog();
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
        var filename = path.join(folder, prop + '.json');
        fs.writeFile(filename, JSON.stringify(obj), function (err) {
            if (err) {
                console.error('Failed to write file ' + filename + ' err:', err);
            }
            callback(err);
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
                console.log('close pers');
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

    function _parseProp(prop) {
        if (!prop || typeof prop !== 'string' || prop.length < 1) {
            throw new Error('invalid property path');
        }
        return prop.split('.');
    }

    // Start watching directory changes.
    function _startWatchdog() {
        if (!watcher) {
            watcher = fs.watch(folder, options, function (event, filename) {
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
                _loadData(filename, data);
            }
            catch (e) {
                console.info('Object ' + filename + ' was not yet fully written, exception:', e);
                // There will be another notification of change when the last write to file is completed.
                // Meanwhile leave the previous value.
                callback(e);
                return;
            }
            callback();
        });
    }

    function _loadFileSync(filename) {
        var data = fs.readFileSync(path.join(folder, filename));
        // If file is written at the same time, this may raise exception. Since synchronous version is not
        // used in serious deployment scenarios, this is not important.
        _loadData(filename, data);
    }

    function _loadData(filename, data) {
        var obj = JSON.parse(data);
        var props = filename.split('.');
        // Don't count suffix (.json).
        props.pop();
        _traverse(props, obj, null);
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
