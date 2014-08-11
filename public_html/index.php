<html>
<head>
<link rel="stylesheet" href="style.css" type="text/css" />
<title>Test interface</title>
</head>
<body>
<a href="results/">view log files</a>
<h3>click a link to test a server</h3>
<?php
foreach (array('cassandra','riak','mongo','voldemort') as $arg) {
	print <<<HTML
<a href="test.php?test=$arg">test $arg</a>
<br>
HTML;
}
?>
</body>
</html>

