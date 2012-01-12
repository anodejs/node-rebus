var testCase = require('nodeunit').testCase;
var path = require('path');
var rimraf = require('rimraf');
var rebus = require('../lib/rebus');

module.exports = testCase({

    setUp: function (callback) {
        this.folder = path.join(process.env.TMP || process.env.TMPDIR, 'rebus', Math.round(Math.random() * 100000).toString());
        console.log('Folder:' + this.folder);
        callback();
    },

    tearDown: function (callback) {
        rimraf(this.folder, function (err) {
            callback(err);
        });
    },

    // Just adhoc scenario used during development.
    adhoc: function (test) {
        var self = this;
        rebus.start(self.folder, function (err, rebus1) {
            console.log('started rebus1');
            test.ok(!err, 'failed to start the 1st rebus instance');
            if (!rebus1) {
                test.done();
                return;
            }
            rebus1.publish('x.k.a', { f1: 'kuku' }, function (err) {
                test.ok(!err, 'failed to publish x.k.a for the 1st time');
                console.log('published x.k.a');
                rebus1.publish('x.k.b', { f2: 'muku' }, function (err) {
                    test.ok(!err, 'failed to publish x.k.b for the 1st time');
                    var xk;
                    console.log('published x.k.b');
                    console.log('subscribe x.k');
                    var notification1 = rebus1.subscribe('x.k', function (obj) {
                        console.log('Notification from rebus1 for x.k:', obj);
                        xk = obj;
                    });
                    // Set notifications on inside of an object
                    var xkbf2;
                    console.log('subscribe x.k.b.f2');
                    var notification2 = rebus1.subscribe('x.k.b.f2', function (obj) {
                        console.log('Notification from rebus1 for x.k.b.f2:', obj);
                        xkbf2 = obj;
                    });
                    var xkaf1;
                    console.log('subscribe x.k.a.f1');
                    var notification3 = rebus1.subscribe('x.k.a.f1', function (obj) {
                        console.log('Notification from rebus1 for x.k.a.f1:', obj);
                        xkaf1 = obj;
                    });
                    // start again and see the published object in there.
                    rebus.start(self.folder, function (err, rebus2) {
                        console.log('started rebus2');
                        test.ok(!err, 'cannot start another instance of rebus');
                        console.log('going to change x.k.a');
                        rebus2.publish('x.k.a', { f3: 'junk' }, function (err) {
                            console.log('changed x.k.a');
                            test.ok(!err, 'cannot publish on another instance of rebus');
                            setTimeout(function () {
                                console.log('xk:', xk);
                                console.log('xkbf2:', xkbf2);
                                console.log('xkaf1:', xkaf1);
                                // verify final state of the objects
                                test.ok(!xkaf1, 'x.k.a.f1 was deleted and hence not defined');
                                test.equal(xkbf2, 'muku');
                                test.deepEqual(xk, { a: { f3: 'junk' }, b: { f2: 'muku'} });
                                console.log('commence tear down');
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
    },

    // Publish and subscribe simple object on depth 1.
    firstLevel: function (test) {
        var self = this;
        rebus.start(self.folder, function (err, rebus1) {
            test.ok(!err, 'failed to start empty instance');
            test.ok(rebus1, 'got the 1st rebus instance');
            var obj3;
            var handler3 = rebus1.subscribe('p1', function (obj) {
                console.log('notification3 p1:', obj);
                obj3 = obj;
            });
            var handler1 = rebus1.subscribe('p1', function (obj) {
                console.log('notification1 p1:', obj);
                test.deepEqual(obj, {}, 'should receive empty object if nothing was published');
                test.ok(handler1, 'handler should be set when notification is called');
                handler1.dispose();
                // No more notifications should arrive from handler1 after dispose.
                handler1 = null;
                rebus1.publish('p1', 'something1', function (err) {
                    test.ok(!err, 'failed to publish');
                    rebus.start(self.folder, function (err, rebus2) {
                        test.ok(!err, 'failed to start non-empty instance');
                        test.ok(rebus2, 'got the 2nd rebus instance');
                        var obj4;
                        var handler4 = rebus2.subscribe('p1', function (obj) {
                            console.log('notification4 p1:', obj);
                            obj4 = obj;
                        });
                        var handler2 = rebus2.subscribe('p1', function (obj) {
                            console.log('notification2 p1:', obj);
                            test.ok(handler2, 'handler should be set when notification is called');
                            test.equal(obj, 'something1');
                            handler2.dispose();
                            // No more notifications should arrive from handler2 after dispose.
                            handler2 = null;
                            setTimeout(function () {
                                // Check eventual consistency.
                                test.equal(obj3, 'something1');
                                test.equal(obj4, 'something1');
                                // dispose only one of handlers and leave the other not disposed.
                                handler3.dispose();
                                rebus2.stop();
                                rebus1.stop();
                                test.done();
                            }, 200);
                        });
                    });
                });
            });
        });
    },

    // Check notifications are called for a subtree of changed object.
    subtreeNotifications: function (test) {
        var self = this;
        rebus.start(self.folder, function (err, rebus1) {
            test.ok(!err, 'failed to start empty instance');
            test.ok(rebus1, 'got the 1st rebus instance');
            var ab1c1;
            rebus1.subscribe('a.b1.c1', function (obj) {
                console.log('a.b1.c1:', obj);
                ab1c1 = obj;
            });
            var ab1c2;
            rebus1.subscribe('a.b1.c2', function (obj) {
                console.log('a.b1.c2:', obj);
                ab1c2 = obj;
            });
            var ab2c3;
            rebus1.subscribe('a.b2.c3', function (obj) {
                console.log('a.b2.c3:', obj);
                ab2c3 = obj;
            });
            var ab2c4;
            rebus1.subscribe('a.b2.c4', function (obj) {
                console.log('a.b2.c4:', obj);
                ab2c4 = obj;
            });
            var ab1;
            rebus1.subscribe('a.b1', function (obj) {
                console.log('a.b1:', obj);
                ab1 = obj;
            });
            var ab2;
            rebus1.subscribe('a.b2', function (obj) {
                console.log('a.b2:', obj);
                ab2 = obj;
            });
            var a;
            rebus1.subscribe('a', function (obj) {
                console.log('a:', obj);
                a = obj;
            });
            var ab1c1d1;
            rebus1.subscribe('a.b1.c1.d1', function (obj) {
                console.log('a.b1.c1.d1:', obj);
                ab1c1d1 = obj;
            });
            rebus1.publish('a.b1', { c1: 'l1', c2: 'l2' });
            rebus1.publish('a.b2', { c4: 'l4', c5: 'l5' });

            setTimeout(function () {
                // Validate eventual consistency.
                test.equal(ab1c1, 'l1');
                test.equal(ab1c2, 'l2');
                test.ok(!ab2c3, 'Not part of the object');
                test.equal(ab2c4, 'l4');
                test.deepEqual(ab1, { c1: 'l1', c2: 'l2' });
                test.deepEqual(ab2, { c4: 'l4', c5: 'l5' });
                test.deepEqual(a, { b1: { c1: 'l1', c2: 'l2' }, b2: { c4: 'l4', c5: 'l5'} });
                test.ok(!ab1c1d1, 'Not part of the object');
                rebus1.stop();
                test.done();
            }, 200);
        });
    }
});