var testCase = require('nodeunit').testCase;
var path = require('path');
var rimraf = require('rimraf');
var rebus = require('../lib/rebus');

module.exports = testCase({

    setUp: function (callback) {
        var self = this;
        self.folder = path.join(process.env.TMP || process.env.TMPDIR, Math.round(Math.random() * 100000).toString());
        rebus.start(self.folder, function (err, result) {
            self.rebus = result;
            callback(err);
        });
    },

    tearDown: function (callback) {
        var self = this;
        rimraf(self.folder, function (err) {
            callback(err);
        });
    },

    publish: function (test) {
        var self = this;
        self.rebus.publish('x.a', { f1: 'kuku' }, function (err) {
            self.rebus.publish('x.b', { f2: 'muku' }, function (err) {
                // start again and see the published object in there.
                rebus.start(self.folder, function (err) {
                    test.ok(!err, 'cannot start another instance of rebus');
                    test.done();
                });
            });
        });
    }
});