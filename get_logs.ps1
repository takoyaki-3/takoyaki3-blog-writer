
$logGroupsJson = aws logs describe-log-groups --query "logGroups[?contains(logGroupName, 'GenerationWorkerFn')].logGroupName" --output json
$logGroups = $logGroupsJson | ConvertFrom-Json
$groupName = $logGroups[0]
Write-Host "Log Group: $groupName"

$streamsJson = aws logs describe-log-streams --log-group-name $groupName --order-by LastEventTime --descending --limit 1 --query "logStreams[0].logStreamName" --output json
$streamName = $streamsJson | ConvertFrom-Json
Write-Host "Log Stream: $streamName"

aws logs get-log-events --log-group-name $groupName --log-stream-name $streamName --limit 20
