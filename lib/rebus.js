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

            // Upon success, return rebus instance.
            function success(callback) {
                console.log('shared:', JSON.stringify(shared));
                callback(null, { publish: publish, subscribe: subscribe });
            }

            if (!folder) {
                folder = path.join(process.env.TMP || process.env.TMPDIR, 'rebus');
            }

            fs.mkdir(folder, 0777, function (err) {

                if (err && err.code !== 'EEXIST') {
                    console.error('Failed to create folder ' + folder);
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
                            console.log('load ' + filename);
                            var filepath = path.join(folder, filename);
                            fs.readFile(filepath, function (err, data) {
                                if (err) {
                                    console.error('Failed to read file ' + filepath + ' err:', err);
                                    cb(err);
                                    return;
                                }
                                var props = filename.split('.');
                                // Don't count suffix (.json).
                                var length = props.length - 1;
                                var ref = shared;
                                for (var i = 0; i < length; i++) {
                                    var prop = props[i];
                                    if (!ref[prop]) {
                                        ref[prop] = {};
                                    }
                                    if (i === (length - 1)) {
                                        ref[prop] = JSON.parse(data);
                                    }
                                    else {
                                        ref = ref[prop];
                                    }
                                }
                                callback();
                            });
                        });
                    });

                    if (fns.length === 0) {
                        console.log('No object published');
                        success(callback);
                        return;
                    }

                    async.parallel(fns, function (err) {
                        if (err) {
                            callback(err);
                        }
                        else {
                            success(callback);
                        }
                    });
                });
            });

            function publish(prop, obj, callback) {
                console.log('publish ' + prop + ' at ' + folder);

                // Write the object to separate file.
                var filename = path.join(folder, prop + '.json');
                fs.writeFile(filename, JSON.stringify(obj), function (err) {
                    if (err) {
                        console.error('Failed to write file ' + filename + ' err:', err);
                    }
                    callback(err);
                });
            }

            function subscribe(prop, callback) {
                console.log('subscribe ' + prop + ' at' + folder);
            }
        }
    }
} ();