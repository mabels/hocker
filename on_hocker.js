/*
 * Do not use this @home
 * This tool will load a complete couchdb server from files which named .hocker
 * you should give the name on the cmd-line
 */

var sys = require('sys')
var posix = require('fs')
var http = require('http')
var buffer = require('buffer').Buffer

var CouchDB = function(host, port) {
   if (port == null) {port = 5984}
   if (host == null) {host = 'localhost'}
   return http.createClient(port, host)
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

var OnHocker = function(fname, host, port) {
   this.fname = fname
   this.db = "hocker_"+fname.replace(/\.hocker$/,'')
   this.cnt = 0
   var self = this
sys.puts("OnHocker:START:CREATEDB:"+self.db)
   var req = CouchDB(host, port).request('DELETE','/'+self.db+'/', { 'Content-Length': 0 })
   req.write('')
   req.addListener('response',function() {
sys.puts("CREATE DB. " + self.db)
      req = CouchDB().request('PUT','/'+self.db+'/', { 'Content-Length': 0 })
      req.write('')
      req.addListener('response',function() {
//sys.puts("OnHocker:START:OPEN:"+self.fname)
         posix.open(self.fname, process.O_RDONLY, 0644, function(err, fd) {
//sys.puts("OPENED:"+self.fd)
            self.put_object(fd)
         })
      }).end()
   }).end()
}

OnHocker.prototype.put_object = function(fd, self) {
   self = this
   new BufferReader(fd).read(new buffer(16), function(length) {
      if (length == null) { 
         sys.puts('COMPLETED:'+self.cnt+":"+self.db)
         return 
      }
sys.puts("OnHocker.prototype.put_object:"+this.done_read+" : "+length)
      this.read(new buffer(parseInt(length,10)), function(id) {
sys.puts('ID:'+id)
         this.read(new buffer(16), function(length) {
            length = parseInt(length,10)
            this.read(new buffer(length), function(body) {
//sys.puts('PUT:'+length+":"+body)
               body = JSON.parse(body)
               delete body._rev
               body = JSON.stringify(body)
               req = CouchDB().request('PUT','/'+self.db+'/'+id, {'Content-Length': body.length.toString()})
               sys.puts('Body Length: ' + body.length + 'Buffer Length: ' +length.toString())
               req.write(body, 'binary');
               req.addListener('response', function(response) {
sys.puts ('STATUS ' + response.statusCode)
//sys.puts('DONE:'+length+":"+body)
                  self.cnt += 1
                  self.put_object(fd)
               }).end()
            })
         })
      })
   })
}

var couchdb = CouchDB()
for (var i = process.ARGV.length-1; i >= 4; --i) {
   new OnHocker(process.ARGV[i],process.ARGV[2],process.ARGV[3]);
}

