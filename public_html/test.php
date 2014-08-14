<?php
$arg = $_GET['test'];
$title = $arg." jepsen tests";
$heading = $title;
if (file_exists("results/TESTING")) $testing = "test in progress";

print <<<HTML
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>$title</title>
<link rel="stylesheet" href="style.css" type="text/css" />
<!-- script type="text/javascript" src="http://code.jquery.com/jquery-1.11.1.min.js"></script -->
<script type="text/javascript" src="jquery-1.11.1.min.js"></script>
<script type="text/javascript">
jQuery(document).ready( function () {
	upd_status('$arg');
});
function upd_status(test) {
	jQuery.get(
		'status.cgi',
		{test: test},
		function (data) { jQuery('#status').html(data); }
	); 
}
</script>
</head>
<body>
<a href="index.php">home</a>
&nbsp;
<a href="results/">all files</a>
&nbsp;
<a href="javascript: void(0);" 
   onclick="
	if (confirm('Click OK to start the $arg server')) {
		jQuery.get(
			'start-server.cgi', 
			{test: '$arg'}, 
			function (data) { jQuery('#serverout').prop('href',data).html(data); }
		); 
		upd_status('$arg');
	}
	return false;"
>restart $arg server</a>
&nbsp;
<a href="javascript: void(0);" 
   onclick="
	if (confirm('click OK to start a test run')) {
	   jQuery.get(
		'start-test.cgi',
		{ test: '$arg' },
		function (data) { jQuery('#testout').prop('href',data).html(data); } );
	   upd_status('$arg');
	}
	return false;" 
>do $arg tests</a>
&nbsp;
<a href="javascript: void;" 
   onclick="
	jQuery.get(
		'clear-testing.cgi',
		function (data) { jQuery('#testing').html(data).show().fadeOut(2000) }); 
	upd_status('$arg');
	return false;
   "
>clear TESTING lock file</a>
<span id="testing">$testing</span>

<h3>$heading</h3>
Server log: <a id="serverout" href="" target="_blank"></a>
<br><br>
Test log: <a id="testout" href="" target="_blank"></a>
<br>
<h4>status</h4>
<a href="javascript:void(0);" 
   onclick="upd_status('$arg'); return false;"
>refresh</a>
<br>
<pre id=status>
no $arg processes
</pre>
</body>
</html>
HTML;

