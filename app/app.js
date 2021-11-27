var express = require('express');
var app = express();
var path = require('path');
var PORT = 3000;
  
// Static Middleware 
app.use(express.static(path.join(__dirname, 'public')))
    
app.get('/', function(req, res) {
    let options = {
        root: path.join(__dirname + '/public')
    }
    res.sendFile(path.join(__dirname, 'index.html', options));
});
  
app.listen(PORT, function(err){
    if (err) console.log(err);
    console.log("Server listening on PORT", PORT);
});
