var path = require('path');
var fs = require('fs');
var async = require('async');
var mkdirp = require('mkdirp');

// Suffix for all published objects.
var suffix = '.json';
// To avoid collision with objects property names, define string that cannot collide.
var nfs = 'yosefsaysthiscannotbepropertyname';

module.exports = function () {

    return {

        /*
        // Rebus module has only function start.
        // The function returns Rebus instance with 3 methods in its interface:
        // - stop
        // - publish
        // - subscribe
        */

        // Create an instance of rebus in specified folder.
        // folder - where the shared files are kept
        // callback(err, rebusInstance) - instance object that can be used for publishing and subsciptions.
        start: function (folder, callback) {

            if (!callback) {
                throw { err: 'callback is required' };
            }
            if (typeof callback !== 'function') {
                throw { err: 'invalid callback' }
            }
            if (!folder) {
                callback({ err: 'invalid property path' });
            }

            // The shared object built from all published objects.
            var shared = {};
            // Notification handling metadata.
            var meta = {};
            // Folder watching handler.
            var watcher;
            // Next free id for notification binding.
            var freeId = 0;

            mkdirp(folder, function (err) {

                if (err) {
                    console.error('Failed to create folder ' + folder + ' err:', err);
                    callback(err);
                    return;
                }

                // Upon start load all data and create full shared object.
                fs.readdir(folder, function (err, files) {
                    if (err) {
                        console.error('Failed to read filder ' + folder + ' err:', err);
                        callback(err);
                        return;
                    }

                    async.forEach(files, _loadFile, function (err) {
                        if (err) {
                            console.warn('Loading all files was not smooth, err:', err);
                        }
                        // Regardless errors, start watching the directory.
                        _startWatchdog(callback);
                    });
                });
            });

            /*
            // Rebus instance.
            //
            */

            // Stop listening for changes.
            function stop() {
                watcher.close();
            }

            // Publish object.
            // prop - dotted property path.
            // obj - JSON object to publish.
            // callback(err) - completion,
            function publish(prop, obj, callback) {
                callback = callback || function () { };
                if (typeof callback !== 'function') {
                    throw { err: 'invalid callback' }
                }
                if (!prop || typeof prop !== 'string' || prop.length < 1) {
                    throw { err: 'invalid property path' }
                }
                // Write the object to separate file.
                var filename = path.join(folder, prop + '.json');
                fs.writeFile(filename, JSON.stringify(obj), function (err) {
                    if (err) {
                        console.error('Failed to write file ' + filename + ' err:', err);
                    }
                    callback(err);
                });
            }

            // Subsribe on changes.
            // prop - dotted property path specifies object that triggers notification.
            // notification - called upon changes in monitor subobject.
            // Return - notification handler. Used to stop notification calls (disposing the handler).
            function subscribe(prop, notification) {
                if (!prop || typeof prop !== 'string' || prop.length < 1) {
                    throw { err: 'invalid property path' }
                }
                if (!notification || typeof notification !== 'function') {
                    throw { err: 'invalid notification callback' }
                }
                var props = prop.split('.');
                return _traverse(props, null, notification);
            }

            /*
            // Notification handler.
            //
            */

            // Dispose the notificaiton handler.
            function dispose() {
                delete this[nfs][this.id];
            }

            /*
            // Private functions.
            //
            */

            // Start watching directory changes. When watchdog is installed, create rebus instance and return
            // it via callback.
            function _startWatchdog(callback) {
                watcher = fs.watch(folder, function (event, filename) {
                    if (event === 'change') {
                        // On every change load the changed file. This will trigger notifications for interested
                        // subscribers.
                        _loadFile(filename);
                    }
                });

                // The instance has access to instance public functions.
                callback(null, { stop: stop, publish: publish, subscribe: subscribe });
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
                        var obj = JSON.parse(data);
                    }
                    catch (e) {
                        console.info('Object ' + filename + ' was not yet fully written, exception:', e);
                        // There will be another notification of change when the wrie will be completed.
                        // Meanwhile leave the previous value.
                        callback(e);
                        return;
                    }
                    var props = filename.split('.');
                    // Don't count suffix (.json).
                    props.pop();
                    _traverse(props, obj, null);
                    callback();
                });
            }

            // Traverse the shared object acording to property path.
            // If object specified, call all affected notifications. Those are notifications along the property path
            // and in the subtree at the end of the path.
            // props - the path in the shared object.
            // obj - if defined, pin the object at the end of the specified path.
            // notification - if defined, pin the notification at the end of specified path.
            // Returns - if called with notification, returns information where the notification was pinned, so can be
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
                            // Return value indicaes where the notification was pinned.
                            handler = { id: id, dispose: dispose };
                            handler[nfs] = currentmeta[nfs];
                            // Call the notification with initial value of the object.
                            // The 1st notification receives the handler, so that can be used in callback
                            // to dispose.
                            notification(currentobj, handler);
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
        }
    }
} ();