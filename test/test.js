var testCase = require('nodeunit').testCase;
var path = require('path');
var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var fs = require('fs');
var rebus = require('../lib/rebus');

module.exports = testCase({

  setUp: function (callback) {
    this.folder = path.join(process.env.TMP || process.env.TMPDIR, 'rebus', Math.round(Math.random() * 100000).toString());
    console.log('Folder:' + this.folder);
    mkdirp(this.folder, callback);
  },

  tearDown: function (callback) {
    rimraf(this.folder, function (err) {
      callback(err);
    });
  },

  missingfolder: function (test) {
    var rebusNoFolder = rebus('nosuchfolder', function (err) {
      test.ok(err);
      test.done();
    });
  },

  // Just adhoc scenario used during development.
  adhoc: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, { singletons: false }, function (err) {
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
          var rebus2 = rebus(self.folder, { singletons: false }, function (err) {
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
                console.log('value:', JSON.stringify(rebus2.value));
                // verify final state of the objects
                test.ok(!xkaf1, 'x.k.a.f1 was deleted and hence not defined');
                test.equal(xkbf2, 'muku');
                test.deepEqual(xk, { a: { f3: 'junk' }, b: { f2: 'muku'} });
                test.deepEqual(rebus1.value, { x: { k: { a: { f3: 'junk' }, b: { f2: 'muku'}}} });
                test.deepEqual(rebus2.value, { x: { k: { a: { f3: 'junk' }, b: { f2: 'muku'}}} });
                console.log('commence tear down');
                notification3.close();
                notification2.close();
                notification1.close();
                rebus2.close();
                rebus1.close();
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
    var rebus1 = rebus(self.folder, { singletons: false }, function (err) {
      test.ok(!err, 'failed to start empty instance');
      test.ok(rebus1, 'got the 1st rebus instance');
      var obj3;
      var notification3 = rebus1.subscribe('p1', function (obj) {
        console.log('notification3 p1:', obj);
        obj3 = obj;
      });
      var notification1 = rebus1.subscribe('p1', function (obj) {
        console.log('notification1 p1:', obj);
        test.deepEqual(obj, {}, 'should receive empty object if nothing was published');
        test.ok(notification1, 'notification should be set when notification is called');
        notification1.close();
        // No more notifications should arrive from notification1 after close.
        notification1 = null;
        rebus1.publish('p1', 'something1', function (err) {
          test.ok(!err, 'failed to publish');
          var rebus2 = rebus(self.folder, { singletons: false }, function (err) {
            test.ok(!err, 'failed to start non-empty instance');
            test.ok(rebus2, 'got the 2nd rebus instance');
            var obj4;
            var notification4 = rebus2.subscribe('p1', function (obj) {
              console.log('notification4 p1:', obj);
              obj4 = obj;
            });
            var notification2 = rebus2.subscribe('p1', function (obj) {
              console.log('notification2 p1:', obj);
              test.ok(notification2, 'notification should be set when notification is called');
              test.equal(obj, 'something1');
              notification2.close();
              // No more notifications should arrive from notification2 after close.
              notification2 = null;
              setTimeout(function () {
                // Check eventual consistency.
                test.equal(obj3, 'something1');
                test.equal(obj4, 'something1');
                // close only one of notifications and leave the other not closed.
                notification3.close();
                rebus2.close();
                rebus1.close();
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
    var rebusT = rebus(self.folder, function (err) {
      test.ok(!err, 'failed to start empty instance');
      test.ok(rebusT, 'got the 1st rebus instance');
      var ab1c1;
      rebusT.subscribe('a.b1.c1', function (obj) {
        console.log('a.b1.c1:', obj);
        ab1c1 = obj;
      });
      var ab1c2;
      rebusT.subscribe('a.b1.c2', function (obj) {
        console.log('a.b1.c2:', obj);
        ab1c2 = obj;
      });
      var ab2c3;
      rebusT.subscribe('a.b2.c3', function (obj) {
        console.log('a.b2.c3:', obj);
        ab2c3 = obj;
      });
      var ab2c4;
      rebusT.subscribe('a.b2.c4', function (obj) {
        console.log('a.b2.c4:', obj);
        ab2c4 = obj;
      });
      var ab1;
      rebusT.subscribe('a.b1', function (obj) {
        console.log('a.b1:', obj);
        ab1 = obj;
      });
      var ab2;
      rebusT.subscribe('a.b2', function (obj) {
        console.log('a.b2:', obj);
        ab2 = obj;
      });
      var a;
      rebusT.subscribe('a', function (obj) {
        console.log('a:', obj);
        a = obj;
      });
      var ab1c1d1;
      rebusT.subscribe('a.b1.c1.d1', function (obj) {
        console.log('a.b1.c1.d1:', obj);
        ab1c1d1 = obj;
      });
      rebusT.publish('a.b1', { c1: 'l1', c2: 'l2' });
      rebusT.publish('a.b2', { c4: 'l4', c5: 'l5' });

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
        rebusT.close();
        test.done();
      }, 200);
    });
  },

  publishWithoutChange: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, function (err) {
      test.ok(!err, 'failed to start empty instance');
      test.ok(rebus1, 'got the 1st rebus instance');
      var count1 = 0;
      var count2 = 0;
      var count3 = 0;
      rebus1.subscribe('a.c', function (obj) {
        if (obj.b === 'b') {
          if (count1 === 0) {
            // Publish the identical object.
            rebus1.publish('a.c', { b: 'b' });
          }
          count1++;
        }
      });
      var rebus2 = rebus(self.folder, function (err) {
        test.ok(!err, 'failed to start empty instance');
        test.ok(rebus2, 'got the 2nd rebus instance');
        rebus2.subscribe('a.c', function (obj) {
          if (obj.b === 'b') {
            count2++;
          }
        });
      });
      var rebus3 = rebus(self.folder, function (err) {
        test.ok(!err, 'failed to start empty instance');
        test.ok(rebus3, 'got the 3rd rebus instance');
        rebus3.subscribe('a', function (obj) {
          if (obj.c && (obj.c.b === 'b')) {
            count3++;
          }
        });
      });

      rebus1.publish('a.c', { b: 'b' });

      setTimeout(function () {
        // Only one notification should be received after all.
        test.equal(count1, 1);
        test.equal(count2, 1);
        test.equal(count3, 1);
        rebus1.close();
        rebus2.close();
        rebus3.close();
        test.done();
      }, 200);
    });
  },

  modifyRebusObject: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, function (err) {
      test.ok(!err, 'failed to start empty instance');
      test.ok(rebus1, 'got the 1st rebus instance');
      var count1 = 0;
      var count2 = 0;
      var setx = true;
      var setz = true;
      rebus1.subscribe('a.c', function (obj) {
        console.log('rebus1 got', obj);
        count1++;
        if (obj.b === 'b' && setx) {
          setx = false;
          obj.b = 'x';
          rebus1.publish('a.c', obj);
        }
      });

      var rebus2 = rebus(self.folder, function (err) {
        test.ok(!err, 'failed to start empty instance');
        test.ok(rebus2, 'got the 2nd rebus instance');
        rebus2.subscribe('a.c', function (obj) {
          console.log('rebus2 got', obj);
          count2++;
          if (obj.b === 'x' && setz) {
            setz = false;
            var obj = rebus2.value.a.c;
            obj['d'] = 'z';
            rebus2.publish('a.c', obj); 
          }
        });
      });

      rebus1.publish('a.c', { b: 'b' });

      setTimeout(function () {
        test.deepEqual(rebus1.value.a.c, { b: 'x', d: 'z' });
        test.deepEqual(rebus2.value.a.c, { b: 'x', d: 'z' });
        test.equal(count1, 4); // one empty, one 'b', one 'x' and one with d.
        test.equal(count2, 4); // one empty, one 'b', one 'x' and one with d.
        rebus1.close();
        rebus2.close();
        test.done();
      }, 200);
    });
  },

  sync1: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, { singletons: false });
    rebus1.publish('a.b', { c1: 'x', c2: 'y' });
    rebus1.close();
    // Give grace period for fs to really write the file.
    setTimeout(function () {
      // Here goes synchronous usage.
      var rebus2 = rebus(self.folder, { singletons: false });
      test.deepEqual(rebus2.value.a.b, { c1: 'x', c2: 'y' });
      test.deepEqual(rebus2.value, { a: { b: { c1: 'x', c2: 'y'}} });
      rebus2.close();
      test.done();
    }, 200);
  },

  sync2: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, { singletons: false });
    rebus1.publish('a.b', { c1: 'x', c2: 'y' }, function (err) {
      var notification1 = rebus1.subscribe('a', function (obj) {
        try {
          var rebus2 = rebus(self.folder, { singletons: false });
          test.deepEqual(rebus2.value, { a: { b: { c1: 'x', c2: 'y'}} });
          // If got here, rebus should be loaded successfully.
          notification1.close();
          rebus1.close();
          rebus2.close();
          test.done();
        }
        catch (e) {
          console.log('exception on instantiating sync rebus:', e);
        }
      });
    });
  },

  // test syncrounous loading from existing rebus folder.
  sync3: function (test) {
    var self = this;
    var rebusT = rebus(path.join(__dirname, 'testRebus'));
    test.deepEqual(rebusT.value, { a: { b: { c1: 'x', c2: 'y'} }, c: { d: {}} });
    rebusT.close();
    test.done();
  },

  singleton: function (test) {
    var self = this;
    var folder = path.join(__dirname, 'testRebus');
    var rebus1 = rebus(folder, function (err) {
      test.ok(!err, 'should get rebus instance');
      var rebus2 = rebus(folder);
      test.ok(rebus1 === rebus2, 'should be the same instance for the same folder');
      test.deepEqual(rebus2.value, { a: { b: { c1: 'x', c2: 'y'} }, c: { d: {}} });
      rebus1.close();
      // The 2nd close should do nothing.
      rebus2.close();
      test.done();
    });
  },

  // Should be a notification when the rebus in consistent state, even there are some
  // notifications while the state is transient.
  consistentNotificaiton: function (test) {
    var self = this;
    var rebus1 = rebus(self.folder, { singletons: false });
    var rebuses = [];
    var notification1 = rebus1.subscribe('a', function (obj) {
      var rebus2;
      try {
        rebus2 = rebus(self.folder, { singletons: false });
        test.deepEqual(rebus2.value, { a: { b: { c1: 'x', c2: 'y'}} });
      }
      catch (e) {
        // No problem. More notifications will come.
        console.log('exception on instantiating sync rebus:', e);
        // This instance of rebus was not loaded. Save it, to close later.
        rebuses.push(rebus2);
        return;
      }
      // If got here, rebus should be loaded successfully.
      notification1.close();
      rebus1.close();
      rebus2.close();
      // Close all instances that failed to initialize due to transient state.
      // They can be initialized without exception now.
      rebuses.forEach(function (r) { r.close(); });
      test.done();
    });
    rebus1.publish('a.b', { c1: 'x', c2: 'y' });
  },

  loaderIncompleteFile: function (test) {
    var self = this;
    var value = '{ "a": 1';
    var loaded = false;
    var completed = 0;
    function complete() {
      if (++completed === 2) {
        test.done();
      }
    }
    fs.writeFile(path.join(self.folder, 'b.json'), value, function (err) {
      test.ok(!err, 'should write partial file');
      var rebusT = rebus(self.folder, function (err) {
        test.ok(!err, 'should get rebus instance');
        loaded = true;
        test.deepEqual(rebusT.value, { b: { a: 2} });
        rebusT.close();
        complete();
      });
      setTimeout(function () {
        test.ok(!loaded, 'the file is not complete, should be waiting for correct file');
        value = '{ "a": 2 }';
        fs.writeFile(path.join(self.folder, 'b.json'), value, function (err) {
          test.ok(!err, 'should write full file');
          complete();
        });
      }, 300);
    });
  }
});