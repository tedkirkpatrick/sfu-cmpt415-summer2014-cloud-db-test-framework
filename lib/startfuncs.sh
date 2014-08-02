function htmlpage
{
cat <<HTML
<html>
<head>
<title>$title</title>
<script src="http://code.jquery.com/jquery-1.11.1.min.js"></script>
</head>
<body>
<a href="index.php">home</a>
&nbsp;
<a href="results/">all files</a>
&nbsp;
<a href="start-server.cgi?test=$arg">restart $arg server</a>
&nbsp;
<a href="start-test.cgi?test=$arg" onclick="confirm('click OK to start a test run');">do $arg tests</a>
&nbsp;
<a href="clear-testing.cgi">clear TESTING file</a>
<h3>$heading</h3>
<a href="$outfile">$outfile</a>
<h4>status</h4>
<a href="javascript:void(0);" onclick="jQuery.get('status.cgi',{test: '$arg'},function (data) { jQuery('#status').html(data); }); return false;">refresh</a>
<br>
<pre id=status>
$status
</pre>
</body>
</html>
HTML

}

