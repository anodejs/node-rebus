var testCase = require('nodeunit').testCase;
var path = require('path');
var rimraf = require('rimraf');
var rebus = require('../lib/rebus');

module.exports = testCase({

    setUp: function (callback) {
        this.folder = path.join(process.env.TMP || process.env.TMPDIR, Math.round(Math.random() * 100000).toString());
        callback();
    },

    tearDown: function (callback) {
        rimraf(this.folder, function (err) {
            callback(err);
        });
    },

    test1: function (test) {
        var self = this;
        rebus.start(self.folder, function (err, rebus1) {
            test.ok(!err, 'failed to start the 1st rebus instance');
            if (!rebus1) {
                test.done();
                return;
            }
            var handlerXK;
            rebus1.publish('x.k.a', { f1: 'kuku' }, function (err) {
                rebus1.publish('x.k.b', { f2: 'muku' }, function (err) {
                    var notification1 = rebus1.subscribe('x.k', function (obj) {
                        console.log('Notification from rebus1 for x.k:', obj);
                    });
                    // Set notification on inside of an object
                    var notification2 = rebus1.subscribe('x.k.b.f2', function (obj) {
                        console.log('Notification from rebus1 for x.k.b.f2:', obj);
                    });
                    var notification3 = rebus1.subscribe('x.k.a.f1', function (obj) {
                        console.log('Notification from rebus1 for x.k.a.f1:', obj);
                    });
                    // start again and see the published object in there.
                    rebus.start(self.folder, function (err, rebus2) {
                        test.ok(!err, 'cannot start another instance of rebus');
                        rebus2.publish('x.k.a', { f3: 'junk' }, function (err) {
                            test.ok(!err, 'cannot publish on another instance of rebus');
                            setTimeout(function () {
                                notification3.dispose();
                                notification2.dispose();
                                notification1.dispose();
                                rebus2.stop();
                                rebus1.stop();
                                test.done();
                            }, 200);
                        });
                    });
                });
            });
        });
    }
});