/*
 * Do not use this @home
 * This tool will dump a complete couchdb server to files which named .hocker
 */
var sys = require('sys')
var posix = require('fs')
var http = require('http')
var base64 = require('./base64').base64
var buffer = require('buffer').Buffer
var _ = require('./underscore')._

var CouchDB = function(host, port) {
   if (port == null) {port = 5984}
   if (host == null) {host = 'localhost'}
   var ret = new (function() {})();
   ret.host = host;
   ret.port = port;
   ret.http = http;
   ret.request = function() { return this.http.request.apply(this.http, arguments); }
   return ret;
//   return http.createClient(port, host)
}


var BufferWriter = function(fd) {
   this.fd = fd
}

BufferWriter.prototype.write = function(buffer, fn, self) {
   this.written = 0
   this.buffer = buffer
   this.to_write = buffer.length
   this.fn = fn
   self = this
//console.log(this.fd+":"+this.written+":"+buffer.length);
   posix.write(this.fd, buffer, this.written, buffer.length, null, function(error,written) { self.writer(error, written) })
}

BufferWriter.prototype.writer = function(error, written, self) {
   //sys.puts("WRITTEN: " + written + " THIS.WRITTEN: " + this.written + " TO_WRITE: " + this.to_write)
   this.written += written
   if (this.written == this.to_write) {
      this.fn.apply(this, [null])
      return
   }
   posix.write(this.fd, this.buffer, written, this.to_write - written, null, function(error,written) { self.writer(error,written) })
}


var JSONRequest = function() { }

JSONRequest.prototype.request = function(url, fn, obj, self) {
//sys.puts("URL: "+url+" FN: "+fn)
   this.chunks = []
   self = this
//   sys.puts('URL_: ' + url)
   obj.http.request({
     method: 'GET',
     path: url,
     host: obj.http.host,
     port: obj.http.port
   }, function(response) {
      if (response.statusCode == 200) {
         //sys.puts ('RESPONSE CODE ' + response.statusCode)
         response.setEncoding('utf8')
         response.addListener('data', function(chunk) {
            self.chunks.push(chunk)
         })
         response.addListener('end', function() {
//sys.puts("COMPLETE:"+self.chunks.join(''))
            //fn(JSON.parse(self.chunks.join('')), obj)
            fn.apply(self, [JSON.parse(self.chunks.join('')), obj])
         })
      } else {
         sys.puts('SOMETHING IS REALLY WRONG: ' + response.statusCode + ' ' + url)
      }
   }).end()
}

JSONRequest.prototype.request_body = function(url, fn, obj, self) {
//sys.puts("request_body:"+url)
   this.body = []
   self = this
//   sys.puts('URL_: ' + url)
   obj.http.request({
     method: 'GET',
     path: url,
     host: obj.http.host,
     port: obj.http.port
   }, function(response) {
      if (response.statusCode == 200) {
//         sys.puts ('SUCCESS ' + response.statusCode + ' ' + url)
         response.addListener('data', function(chunk) {
            self.body.push(chunk)
         })
         response.addListener('end', function() {
            fn.apply(self, [self.body.join(''), obj])
         })
      } else {
         sys.puts('SOMETHING IS WRONG: ' + response.statusCode + ' ' + url)
         response.addListener('end', function() {
            fn.apply(self, [null, obj])
         })
      }
   }).end()
}

var header = function(obj) {
   var msg = obj.length.toString()
   for(var i = msg.length; i < 16; ++i) { msg+=' ' }
   return msg
}

var remove_attachments = function(obj, data, self) {
   self = this
   sys.puts('ROW: ' + data.rows[data.row].id)
   var json = JSON.parse(obj)
   sys.puts ('Attachments: ' + json._attachments)
   var keys = _(json._attachments).keys()
   var call_it = function(k, att_chunk) {
      att_chunk = []
      data.http.request('GET','/'+data.db+'/'+data.rows[data.row].id+'/'+k)
          .addListener('response',function(response) {
           response.setEncoding('binary')
           sys.puts('GET SUCCESS ' + response.statusCode + ' / ' + k);
            response.addListener('data', function(chunk) {
               att_chunk.push(chunk)
            })
            response.addListener('end',function() {
               json._attachments[k].stub = false
               sys.puts('Stub: '+json._attachments[k].stub)
               json._attachments[k].data = base64.encode(att_chunk.join(''))

               //sys.puts ("haha: "+ sys.inspect(json._attachments[k]))
               //json._attachments.content_type = req.respodnfbgfghkf
               if (keys.length) {
                  call_it(keys.shift())
               } else {
                  sys.puts('Header: '+sys.inspect(data.rows[data.row].id)+' , '+sys.inspect(JSON.stringify(json).length))
                  var msg = header(data.rows[data.row].id)+data.rows[data.row].id+header(JSON.stringify(json))
                  var buffer_head = new buffer(msg)
                  var buffer_body = new buffer(JSON.stringify(json))
                  new BufferWriter(data.file).write(buffer_head,function(){
                     new BufferWriter(data.file).write(buffer_body,function(){
                        data.row += 1
                        if (data.row < data.rows.length) {
                           self.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
                        } else {
                           self.request('/'+data.db+'/_all_docs?limit=1000&skip=1&startkey_docid='+(data.rows[data.rows.length-1].id), all_docs_reader, data)
                        }
                     })
                  })
               }
            })
      }).end()
   }
   call_it(keys.shift())
}

var get_document = function(obj, data, self) {
   self = this
   if (obj != null){
      //sys.puts('ROW: ' + data.rows[data.row].id)
      var msg = header(data.rows[data.row].id)+data.rows[data.row].id+header(obj)
      var buffer_head = new buffer(msg)
      var buffer_body = new buffer(obj)
      
   //sys.puts("MSG: " + msg + " OBJ: " + obj)
      new BufferWriter(data.file).write(buffer_head,function(){
         new BufferWriter(data.file).write(buffer_body,function(){
            data.row += 1
            if (data.row < data.rows.length) {
               self.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
            } else {
               self.request('/'+data.db+'/_all_docs?limit=1000&skip=1&startkey_docid='+(data.rows[data.rows.length-1].id), all_docs_reader, data)
            }
         })
      })
   } else {
      //try without attachments
      if (data.row < data.rows.length) {
         self.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=false', remove_attachments, data)
      } else {
         self.request('/'+data.db+'/_all_docs?limit=1000&skip=1&startkey_docid='+(data.rows[data.rows.length-1].id), all_docs_reader, data)
      }
   }
}

var all_docs_reader = function(obj, data) { 
//sys.puts("ALL DATA: "+ sys.inspect(data))
if (obj.rows.length > 0) {
      data.cnt += obj.rows.length
      sys.puts("next:"+data.db+"="+data.cnt+"=>"+(obj.rows[obj.rows.length-1].id))
      data.row = 0
      data.rows = obj.rows
      this.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
   } else {
      posix.close(data.file, function() {
         //sys.puts("COMPLETE:"+data.db)
      })
   }
}

//var split_db_name = function(db){
//   var Suche = /([\w\.]*):(\d+\d+\d+\d+)\/(\w*)/
//   var Ergebnis = Suche.exec(db)
//   if (Ergebnis){
//      host = Ergebnis[1]
//      port = Ergebnis[2]
//      db_name = Ergebnis[3]
//   } else {
//      host = null
//      port = null
//      db_name = db
//   }
//   return [host, port, db_name]
//}

var Hocker = function(db,host,port) {
   this.db = db
   this.http = CouchDB(host,port)
   var self = this
   posix.open(this.db+".hocker", "w", 0644, function(err, fd) {
      if (err) {
        sys.puts("OPEN:FAILED:"+this.db+":"+err);
        return;
      }
      self.file = fd
      self.err = err
      self.cnt = 0
      sys.puts("MADE .hocker for: " + self.err +" "+self.file+"="+self.db)
      new JSONRequest().request('/'+self.db+'/_all_docs?limit=1000', all_docs_reader, self)
   })
}

for (var i = process.ARGV.length-1; i >= 4; --i) {
   new Hocker(process.ARGV[i],process.ARGV[2],process.ARGV[3])
}
/*
var all_dbs = new JSONRequest() 
all_dbs.request('/_all_dbs', function(dbs) {
   for (var i = dbs.length-1; i >= 0; --i) {
      new Hocker(dbs[i])
   }
}, {http: CouchDB()} )
*/

