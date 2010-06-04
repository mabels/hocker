/*
 * Do not use this @home
 * This tool will dump a complete couchdb server to files which named .hocker
 */
var sys = require('sys')
var posix = require('fs')
var http = require('http')
Buffer = require('buffer').Buffer

var CouchDB = function() {
   return http.createClient(5984, '172.16.143.8')
}

var JSONRequest = function() { }

JSONRequest.prototype.request = function(url, fn, obj, self) {
//sys.puts("URL: "+url+" FN: "+fn)
   this.chunks = []
   self = this
   obj.http.request('GET',url).addListener('response',function(response) {
      if (response.statusCode == 200) {
         response.setEncoding('utf8')
         response.addListener('data', function(chunk) {
            self.chunks.push(chunk)
         })
         response.addListener('end', function() {
sys.puts("COMPLETE:"+self.chunks.join(''))
            //fn(JSON.parse(self.chunks.join('')), obj)
            fn.apply(self, [JSON.parse(self.chunks.join('')), obj])
         })
      }
   }).end()
}

JSONRequest.prototype.request_body = function(url, fn, obj, self) {
sys.puts("request_body:"+url)
   this.body = []
   self = this
   obj.http.request('GET',url).addListener('response',function(response) {
      if (response.statusCode == 200) {
         response.addListener('data', function(chunk) {
            self.body.push(chunk)
         })
         response.addListener('end', function() {
            fn.apply(self, [self.body.join(''), obj])
         })
      }
   }).end()
}
var header = function(obj) {
   var msg = obj.length.toString()
   for(var i = msg.length; i < 16; ++i) { msg+=' ' }
   return msg
}


var get_document = function(obj, data, self) {
   self = this
sys.puts("get_document_write_header:start:"+data.file+":"+data.rows[data.row].id)
   var msg = header(data.rows[data.row].id)+data.rows[data.row].id+header(obj)
   sys.puts("MSG: " + msg + " OBJ: " + obj + " Obj_type: " + obj)
    var x = posix.write(data.file, new Buffer(msg), 0, msg.length, null, function(err, written) {


sys.puts ("ERR: " + err + " WRITTEN: " + written + " LENGTH: " + msg.length)
   
//sys.puts("get_document_write_header:done")
//sys.puts("get_document_write_data:start")
      sys.puts("***OBJ-LENGTH: " + obj.length)
      posix.write(data.file, new Buffer(obj), 0, obj.length, null, function() {
sys.puts("get_document_write_data:done") //ad(parseInt(length),
         data.row += 1
         if (data.row < data.rows.length) {
            self.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
         } else {
            self.request('/'+data.db+'/_all_docs?limit=1000&skip=1&startkey_docid='+(data.rows[data.rows.length-1].id), all_docs_reader, data)
        }
      })
   })
   sys.puts("WRITE RES: " + x)
}

var all_docs_reader = function(obj, data) { 
sys.puts("ALL DATA: "+data)
if (obj.rows.length > 0) {
      data.cnt += obj.rows.length
      sys.puts("next:"+data.db+"="+data.cnt+"=>"+(obj.rows[obj.rows.length-1].id))
      data.row = 0
      data.rows = obj.rows
      this.request_body('/'+data.db+'/'+data.rows[data.row].id+'?attachments=true', get_document, data)
   } else {
      posix.close(data.file, function() {
         sys.puts("COMPLETE:"+data.db)
      })
   }
}


var Hocker = function(db) {
   this.db = db
   this.http = CouchDB()
   var self = this
   posix.open(this.db+".hocker", process.O_WRONLY | process.O_TRUNC | process.O_CREAT, 0644, function(err, fd) {
      self.file = fd
      self.err = err
sys.puts("MADE .hocker for: " + self.err +" "+self.file+"="+self.db)
      self.cnt = 0
      new JSONRequest().request('/'+self.db+'/_all_docs?limit=1000', all_docs_reader, self)
   })
}

for (var i = process.ARGV.length-1; i >= 2; --i) {
   new Hocker(process.ARGV[i])
}
/*
var all_dbs = new JSONRequest() 
all_dbs.request('/_all_dbs', function(dbs) {
   for (var i = dbs.length-1; i >= 0; --i) {
      new Hocker(dbs[i])
   }
}, {http: CouchDB()} )
*/

