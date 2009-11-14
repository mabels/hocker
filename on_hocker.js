/*
 * Do not use this @home
 * This tool will load a complete couchdb server from files which named .hocker
 * you should give the name on the cmd-line
 */

var sys = require('sys')
var posix = require('posix')
var http = require('http')

var CouchDB = function() {
   return http.createClient(5984, 'localhost') 
}

var BufferReader = function(fd) {
   this.fd = fd
}

BufferReader.prototype.read = function(toread, fn, self) {
   this.toread = toread
   this.done_read = 0
   this.buffer = []
   this.toread = toread
   this.fn = fn
   self = this
//sys.puts('BufferReader.prototype.read:'+this.fd+":"+this.toread)
   posix.read(this.fd, this.toread).addCallback(function(d,r) { self.reader(d,r) })
}

BufferReader.prototype.reader = function(data, read, self) {
   if (read == 0) { 
      this.fn.apply(this, [null])
      return
   }
   this.done_read += read
   this.buffer.push(data) 
   if (this.done_read == this.toread) { 
//sys.puts("BufferReader.prototype.reader:apply="+this.done_read)
      this.fn.apply(this, [this.buffer.join('')])
   } else {
      self = this
      posix.read(this.fd, this.toread - this.done_read).addCallback(function(d,r) { self.reader(d,r) })
   }
}


var OnHocker = function(fname) {
   this.fname = fname
   this.db = "hocker_"+fname.replace(/\.hocker$/,'')
   this.cnt = 0
   var self = this
sys.puts("OnHocker:START:CREATEDB:"+self.db)
   var req = CouchDB().del('/'+self.db+'/', { 'Content-Length': 0 })
   req.sendBody('')
   req.finish(function() { 
      req = CouchDB().put('/'+self.db+'/', { 'Content-Length': 0 })
      req.sendBody('')
      req.finish(function() { 
//sys.puts("OnHocker:START:OPEN:"+self.fname)
         posix.open(self.fname, process.O_RDONLY, 0644).addCallback(function(fd) {
//sys.puts("OPENED:"+self.db)
            self.put_object(fd)
         })
      })
   })
}
OnHocker.prototype.put_object = function(fd, self) {
   self = this
   new BufferReader(fd).read(16, function(length) {
      if (length == null) { 
         sys.puts('COMPLETED:'+self.cnt+":"+self.db)
         return 
      }
//sys.puts("OnHocker.prototype.put_object:"+this.done_read)
      this.read(parseInt(length), function(id) {
//sys.puts('ID:'+id)
         this.read(16, function(length) {
            length = parseInt(length)
            this.read(length, function(body) {
//sys.puts('PUT:'+length+":"+body)
               req = CouchDB().put('/'+self.db+'/'+id, {'Content-Length': length.toString()})
               req.sendBody(body, 'binary');
               req.finish(function(response) {
//sys.puts('DONE:'+length+":"+body)
                  self.cnt += 1
                  self.put_object(fd)       
               })
            })
         })
      })
   })
}

var couchdb = CouchDB()
for (var i = process.ARGV.length-1; i >= 2; --i) {
   new OnHocker(process.ARGV[i])
}

