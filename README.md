# Rumcask [![Build Status](https://travis-ci.org/bsm/rumcask.png)](https://travis-ci.org/bsm/rumcask)

Rumcask is an implementation of a key-value store, inspired by ideas
from bitcask, sparkey and leveldb. It can map byte array keys to byte
array values.

## Features

* Written in pure [Go](http://golang.org), no dependencies beyond stdlib.
* Uses an in-memory hash index for key-storage by default, which means that all keys must fit in memory (similar to Bitcask).
* Supports alternative key-storage implementations (e.g. disk persistence, iteration, etc).
* Each DB is stored on disk, in multiples files within a single directory (similar to LevelDB).
* Databases are thread-safe but locked to a single OS process (similar to LevelDB).
* Support for multiple, concurrent readers.
* Data is always appended and never replaced.
* Efficient and configurable compaction (TODO).

## Documentation

Check out the full API on [godoc.org](http://godoc.org/github.com/bsm/rumcask).

## Licence (MIT)

```
Copyright (c) 2014 Black Square Media

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

