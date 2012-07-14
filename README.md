# rebus - Reactive Pub/Sub Bus

[![Build Status](https://secure.travis-ci.org/anodejs/node-rebus.png)](http://travis-ci.org/anodejs/node-rebus)

## Introduction

Rebus allows sharing JSON objects between multiple nodejs processes,
running on the same host. It uses file system to share state.
Objects published via rebus remain persistent.
Rebus supports change notifications (subscriptions) on any sub-tree
of the shared state. Therefore, it can be used as light-weight pubsub
system in scope of one host.

Applications are responsible to devide name space and to avoid publishing
overlapping objects. E.g. if one applicaiton would publish object x.y.z and
another application will publish oject x.y, the result will be unpredictable.
It is OK for one application to publish object x.y and to other application to
publish object x.z, as those 2 are not overlapping.

## Usage

The following demonstrates usage of rebus.

```javascript
var rebus = require('rebus');
// Create rebus instance, specifying the directory where shared state
// is maintained. Application communicating on this bus instantiate
// rebus in the same directory.
var bus = rebus(directoryName, function(err) {
  // The bus is initialized and includes current shared state.
  console.log('the entire shared state is:', bus.value);
  // Can start listening on changes for a particular object.
  var notification = bus.subscribe('some.name.space.x.y.z', function(obj) {
    // Got notification about object being changed by some publisher.
    console.log('some.name.space.x.y.z changed and its value is:', obj);
    console.log(
     'the same object can be accessed as:',
     bus.value.some.name.space.x.y.z);
    console.log('the parent object is:', bus.value.some.name.space.x.y);
  });
  // Publish an object.
  bus.publish(
    'some.other.name.space',
    { x: 'this', y: ['is', 'some', 'object'] },
    function(err) {
     console.log(
       'published some other object and its value now:',
       bus.value.some.other.name.space);
  });
  // Cleanup
  notification.close();
  bus.close();
});
```
Rebus can be instantiated and used synchronously:

```javascript
var rebus = require('rebus');
var bus = rebus(directoryName);
// Read.
console.log('the entire shared state is:', bus.value);
// Write.
bus.publish('x', { ... });
// bus.value.x is not necessary the one assigned as not used in publish
// completion. However, it is still can be used and it includes some value
// that was in x, before or after assignment.
console.log('the value of x now:', bus.value.x);
// Cleanup
bus.close();
```

Note: using rebus value or function before instantiation completion works,
exception may be thrown if other process manipulates shared state concurently.
However, if garanteed that nobody writes into the same directory, rebus will
work correctly and show the right value of the shared object.
The best practice would be to start using rebus upon instantiation completion.

## License

MIT
