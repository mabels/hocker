
var sys = require('sys')
var posix = require('posix')
//var process = require('process')
var http = require('http')
//var joose = require('joose')

var CouchDB = function() {
   return http.createClient(5984, 'localhost') 
}

var JSONRequest = function() { }

JSONRequest.prototype.request = function(url, fn, obj, self) {
//sys.puts("ENTER:"+fn)
   this.chunks = []
   self = this
   CouchDB().get(url).finish(function(response) {
      if (response.statusCode == 200) {
         response.setBodyEncoding('utf8')
         response.addListener('body', function(chunk) {
            self.chunks.push(chunk)
         })
         response.addListener('complete', function() {
//sys.puts("COMPLETE:"+self.chunks.join(''))
            //fn(JSON.parse(self.chunks.join('')), obj)
            fn.apply(self, [JSON.parse(self.chunks.join('')), obj])
         })
      }
   })
}

JSONRequest.prototype.request_body = function(url, fn, obj, self) {
//sys.puts("request_body:"+url)
   this.body = []
   self = this
   CouchDB().get(url).finish(function(response) {
      if (response.statusCode == 200) {
         response.addListener('body', function(chunk) {
            self.body.push(chunk)
         })
         response.addListener('complete', function() {
            fn.apply(self, [self.body.join(''), obj])
         })
      }
   })
}
var header = function(obj) {
   var msg = obj.length.toString()
   for(var i = msg.length; i < 16; ++i) { msg+=' ' }
   return msg
}


var get_document = function(obj, data, self) {
   self = this

//sys.puts("get_document_write_header:start:"+data.file+":"+data.rows[data.row].id)
   var msg = header(data.rows[data.row].id)+data.rows[data.row].id+header(obj)
   posix.write(data.file, msg).addCallback(function() {
//sys.puts("get_document_write_header:done")
//sys.puts("get_document_write_data:start")
      posix.write(data.file, obj).addCallback(function() {
//sys.puts("get_document_write_data:done")
         data.row += 1
         if (data.row < data.rows.length) {
            self.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
         } else {
            self.request('/'+data.db+'/_all_docs?limit=1000&skip=1&startkey_docid='+(data.rows[data.rows.length-1].id), all_docs_reader, data)
        }
      })
   })
}

var all_docs_reader = function(obj, data) { 
   if (obj.rows.length > 0) {
      data.cnt += obj.rows.length
      sys.puts("next:"+data.db+"="+data.cnt+"=>"+(obj.rows[obj.rows.length-1].id))
      data.row = 0
      data.rows = obj.rows
      this.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
   } else {
      posix.close(data.file).addCallback(function() { 
         sys.puts("COMPLETE:"+data.db)
      })
   }
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


var OnHocker = function(db) {
   this.fname = db
   this.db = "hocker_"+db
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
         posix.open(self.fname+".hocker", process.O_RDONLY, 0644).addCallback(function(fd) {
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

