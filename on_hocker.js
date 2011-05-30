/*
 * Do not use this @home
 * This tool will load a complete couchdb server from files which named .hocker
 * you should give the name on the cmd-line
 */

var sys = require('sys')
var posix = require('fs')
var http = require('http')
var util = require('util')
//var buffer = require('buffer').Buffer

var CouchDB = function(host, port) {
   if (port == null) {port = 5984}
   if (host == null) {host = 'localhost'}
   this.port = port
   this.host = host
}

CouchDB.prototype = {
  conn : function() { 
    var self = this;
    return { 
              request: function(method, url, opts, fn) {
                var n = {};
                for (var i in opts) {
                  n[i] = opts[i];
                }
                n.method = method;
                n.port = self.port;
                n.host = self.host;
                n.path = url;
//console.log(method+":"+url+":"+opts+":"+util.inspect(n));
                return http.request(n, fn);
              }
           };
    return http;
  }    
}

var BufferReader = function(fd) {
   this.fd = fd
}

BufferReader.prototype.read = function(buffer, fn, self) {
   this.buffer = buffer
   this.to_read = buffer.length
   this.done_read = 0
   this.fn = fn
   self = this
//sys.puts('BufferReader.prototype.read:'+this.fd+":"+this.toread)
   posix.read(this.fd, this.buffer, this.done_read, this.to_read, null, function(err,bytes_read) { self.reader(err,bytes_read) })
}

BufferReader.prototype.reader = function(err, bytes_read, self) {
   if (bytes_read == 0) {
      this.fn.apply(this, [null])
      return
   }
   this.done_read += bytes_read
   if (this.done_read == this.to_read) {
      this.fn.apply(this, [this.buffer.toString('utf8', 0, this.to_read)])
   } else {
      self = this
      var length = this.to_read - this.done_read
      posix.read(this.fd, this.buffer, this.done_read, length, null, function(err, bytes_read) { self.reader(err,bytes_read) })
   }
}

var OnHocker = function(fname, couchdb) {
   this.fname = fname
   this.db = /* "hocker_"+*/ fname.replace(/\.hocker$/,'')
   this.cnt = 0
   this.couchdb = couchdb
   var self = this
sys.puts("OnHocker:START:CREATEDB:"+self.db)
   var conn = couchdb.conn()
   var req = conn.request('DELETE','/'+self.db+'/', { 'Content-Length': 0 })
   req.write('')
   req.addListener('response',function() {
sys.puts("CREATE DB. " + self.db)
      req = conn.request('PUT','/'+self.db+'/', { 'Content-Length': 0, 'Content-Type': 'application/json' },
        function() {
  //sys.puts("OnHocker:START:OPEN:"+self.fname)
           posix.open(self.fname, "r", function(err, fd) {
  //sys.puts("OPENED:"+self.fd)
              self.put_object(fd, conn)
           })
        });
      req.write('')
      req.end()
   }).end()
}

OnHocker.prototype.put_object = function(fd, conn, self) {
   self = this
   new BufferReader(fd).read(new Buffer(16), function(length) {
      if (length == null) { 
         sys.puts('COMPLETED:'+self.cnt+":"+self.db)
         return 
      }
sys.puts("OnHocker.prototype.put_object:"+this.done_read+" : "+length)
      this.read(new Buffer(parseInt(length,10)), function(id) {
sys.puts('ID:'+id)
         this.read(new Buffer(16), function(length) {
            length = parseInt(length,10)
            this.read(new Buffer(length), function(body) {
//sys.puts('PUT:'+length+":"+body)
               body = JSON.parse(body)
               delete body._rev
               body = JSON.stringify(body)
               req = conn.request('PUT','/'+self.db+'/'+id, {
                    'Content-Length': Buffer.byteLength(body, 'utf-8').toString(),
                    'Content-Type': 'application/json'},
                    function(response) {
sys.puts ('STATUS ' + response.statusCode)
//sys.puts('DONE:'+length+":"+body)
                      self.cnt += 1
                      self.put_object(fd, conn)
                    })
               sys.puts('Body Length: ' + body.length + 'Buffer Length: ' +length.toString())
//console.log(body)
               req.write(body, 'utf-8');
               req.end()
            })
         })
      })
   })
}

var couchdb = new CouchDB(process.ARGV[2],process.ARGV[3])
for (var i = process.ARGV.length-1; i >= 4; --i) {
   new OnHocker(process.ARGV[i], couchdb)
}

