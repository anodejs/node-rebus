var path = require('path');
var fs = require('fs');
var async = require('async');

// Suffix for all published objects.
var suffix = '.json';

module.exports = function () {

    return {

        // Create an instance of rebus in specified folder.
        // If folder name omited, rebus will instancate in rebus subdirectory of temp.
        start: function (folder, callback) {

            console.log('start in ' + folder);

            // The shared object built from all published objects.
            var shared = {};
            // Notification handling metadata.
            var meta = {};
            var watcher;
            var freeId = 0;

            if (!folder) {
                folder = path.join(process.env.TMP || process.env.TMPDIR, 'rebus');
            }

            fs.mkdir(folder, 0777, function (err) {

                if (err && err.code !== 'EEXIST') {
                    console.error('Failed to create folder ' + folder + ' err:', err);
                    callback(err);
                    return;
                }

                // Load shared object.
                fs.readdir(folder, function (err, files) {
                    if (err) {
                        console.error('Failed to read filder ' + folder + ' err:', err);
                        callback(err);
                        return;
                    }

                    var fns = [];

                    files.forEach(function (filename) {
                        fns.push(function (callback) {
                            loadFile(filename, callback);
                        });
                    });

                    if (fns.length === 0) {
                        console.log('No object published');
                        startWatchdog(callback);
                        return;
                    }

                    async.parallel(fns, function (err) {
                        if (err) {
                            console.warn('Loading all files was not smooth, err:', err);
                        }
                        // Regardless errors, start watching the directory.
                        startWatchdog(callback);
                    });
                });
            });

            function startWatchdog(callback) {
                console.log('Start watching when shared state is:', JSON.stringify(shared));

                watcher = fs.watch(folder, function (event, filename) {
                    console.log('Event in ' + folder + '/' + filename + ':', event);
                    if (event === 'change') {
                        onChange(filename);
                    }
                });

                callback(null, { stop: stop, publish: publish, subscribe: subscribe });
            }

            function loadFile(filename, callback) {
                console.log('Load ' + filename);
                if (!callback) {
                    callback = function () { }
                }
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
                        console.warn('Object was not yet fully written, exception:', e);
                        // There will be another notification of change when the wrie will be completed.
                        // Meanwhile leave the previous value.
                        callback(e);
                        return;
                    }
                    var props = filename.split('.');
                    // Don't count suffix (.json).
                    props.pop();
                    traverse(props, obj, null);
                    callback();
                });
            }

            function onChange(filename) {
                console.log(filename + ' changed');
                // Load, update and notify changes.
                loadFile(filename);
            }

            function stop() {
                console.log('Stop watching when shared state is:', JSON.stringify(shared));
                watcher.close();
            }

            function publish(prop, obj, callback) {
                console.log('Publish ' + prop + ' at ' + folder);

                // Write the object to separate file.
                var filename = path.join(folder, prop + '.json');
                fs.writeFile(filename, JSON.stringify(obj), function (err) {
                    if (err) {
                        console.error('Failed to write file ' + filename + ' err:', err);
                    }
                    callback(err);
                });
            }

            function traverse(props, obj, notification) {

                var length = props.length;
                var refobj = shared;
                var refmeta = meta;
                var result = {};

                var fns = [];

                for (var i = 0; i < length; i++) {

                    var prop = props[i];

                    if (!refmeta[prop]) {
                        refmeta[prop] = { notifications: {} };
                    }
                    var currentmeta = refmeta[prop];

                    if (!refobj[prop]) {
                        refobj[prop] = {};
                    }
                    var currentobj = refobj[prop];

                    if (i === (length - 1)) {
                        if (obj) {
                            refobj[prop] = obj;
                            traverseSubtree(currentmeta, obj, fns);
                        }
                        if (notification) {
                            var id = freeId++;
                            currentmeta.notifications[id] = notification;
                            result = { notifications: currentmeta.notifications, id: id };
                            notification(currentobj);
                        }
                    }
                    else if (obj) {
                        // Only if data pushed notifications should be called.
                        pushNotifications(currentmeta, currentobj, fns);
                    }

                    refobj = currentobj;
                    refmeta = currentmeta;
                }

                if (obj) {
                    // Call all notifications.
                    async.parallel(fns);
                }

                return result;
            }

            function pushNotifications(meta, obj, fns) {
                for (var id in meta.notifications) {
                    fns.push(function (i) {
                        return function () {
                            meta.notifications[i](obj);
                        }
                    } (id));
                }
            }

            function traverseSubtree(meta, obj, fns) {
                pushNotifications(meta, obj, fns);
                for (var key in meta) {
                    if (key === 'notifications') {
                        continue;
                    }
                    var subobj;
                    if (obj) {
                        subobj = obj[key];
                    }
                    traverseSubtree(meta[key], subobj, fns);
                }
            }

            function dispose() {
                delete this.notifications[this.id];
            }

            function subscribe(prop, notification) {
                console.log('Subscribe ' + prop + ' at ' + folder);
                var props = prop.split('.');
                var result = traverse(props, null, notification);
                result.dispose = dispose;
                return result;
            }
        }
    }
} ();