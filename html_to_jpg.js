var webshot = require('webshot');

page = 
`<html>
<body>
<table bgcolor="#FFFFFF">
  <tr>
    <th>Firstname</th>
    <th>Lastname</th>
    <th>Age</th>
  </tr>
  <tr>
    <td>Jill</td>
    <td>Smith</td>
    <td>50</td>
  </tr>
  <tr>
    <td>Eve</td>
    <td>Jackson</td>
    <td>94</td>
  </tr>
</table>
</body>
</html>`

var options = {
  windowSize: {
    width: 400
  , height: 600
  }
  ,defaultWhiteBackground: true
  ,quality:100
  ,siteType:'html'
};
 
webshot(page, 'hello_world.jpg', options, function(err) {
  // screenshot now saved to hello_world.png 
});