param(
    [Parameter(Mandatory=$true)][string]$TestBinary,
    [string]$Output = 'update-prepare-benchmark.json',
    [ValidateSet('smoke', 'bulk')][string]$Suite = 'smoke',
    [ValidateRange(1, 10)][int]$Repetitions = 1,
    [string]$Label = 'current',
    [string]$WorkloadName
)
$ErrorActionPreference = 'Stop'
$binary = (Resolve-Path -LiteralPath $TestBinary).Path
$testName = 'table::operations::update_prepare::tests::preparation_memory_benchmark'
$measurements = @()
$workloads = if ($Suite -eq 'bulk') {
    @(
        @{ name = 'narrow-small'; targets = 1048576; updates = 262144; mode = 'shuffled'; payload = 0 },
        @{ name = 'narrow-large'; targets = 4194304; updates = 1048576; mode = 'shuffled'; payload = 0 },
        @{ name = 'narrow-concentrated'; targets = 4194304; updates = 1048576; mode = 'concentrated'; payload = 0 },
        @{ name = 'narrow-dense'; targets = 1048576; updates = 1048576; mode = 'shuffled'; payload = 0 },
        @{ name = 'wide'; targets = 1048576; updates = 262144; mode = 'shuffled'; payload = 1024 },
        @{ name = 'wide-4k'; targets = 1048576; updates = 262144; mode = 'shuffled'; payload = 4096 }
    )
} else {
    foreach ($rows in @(16384, 65536)) {
        foreach ($mode in @('shuffled', 'concentrated')) {
            @{ name = "$rows-$mode"; targets = $rows; updates = $rows / 4; mode = $mode; payload = 0 }
        }
    }
}
if ($WorkloadName) {
    $workloads = @($workloads | Where-Object { $_.name -eq $WorkloadName })
    if ($workloads.Count -eq 0) { throw "Unknown workload for suite ${Suite}: $WorkloadName" }
}
foreach ($workload in $workloads) {
    foreach ($repeat in 1..$Repetitions) {
        $env:TST_UPDATE_TARGET_ROWS = "$($workload.targets)"
        $env:TST_UPDATE_ROWS = "$($workload.updates)"
        $env:TST_UPDATE_SORT_BYTES = if ($Suite -eq 'bulk') { '8388608' } else { '65536' }
        $env:TST_UPDATE_MODE = $workload.mode
        $env:TST_UPDATE_PAYLOAD_BYTES = "$($workload.payload)"
        $stdout = [System.IO.Path]::GetTempFileName()
        $stderr = [System.IO.Path]::GetTempFileName()
        $process = $null
        try {
            $process = Start-Process -FilePath $binary -ArgumentList @('--exact', $testName, '--ignored', '--nocapture') -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
            $peakRss = 0L
            $peakHandles = 0
            $readyRss = $null
            $readyHandles = $null
            while (-not $process.HasExited) {
                $process.Refresh()
                $peakRss = [Math]::Max($peakRss, $process.PeakWorkingSet64)
                $peakHandles = [Math]::Max($peakHandles, $process.HandleCount)
                if ($null -eq $readyRss -and (Select-String -LiteralPath $stdout -Pattern '^UPDATE_BENCH_READY ' -Quiet)) {
                    $readyRss = $process.WorkingSet64
                    $readyHandles = $process.HandleCount
                }
                $process.WaitForExit(20) | Out-Null
            }
            $process.WaitForExit()
            if ($process.ExitCode -ne 0) { throw ((Get-Content -LiteralPath $stdout -Raw) + (Get-Content -LiteralPath $stderr -Raw)) }
            $lines = Get-Content -LiteralPath $stdout
            $ready = ($lines | Where-Object { $_.StartsWith('UPDATE_BENCH_READY ') }).Substring(19) | ConvertFrom-Json
            $result = ($lines | Where-Object { $_.StartsWith('UPDATE_BENCH_RESULT ') }).Substring(20) | ConvertFrom-Json
            $measurement = [ordered]@{ workload = $workload.name; repetition = $repeat; parameters = $ready; result = $result; peak_rss_bytes = $peakRss; sampled_ready_rss_bytes = $readyRss; peak_handles = $peakHandles; sampled_ready_handles = $readyHandles }
            $measurements += $measurement
            $measurement | ConvertTo-Json -Depth 5 -Compress
        } finally {
            Remove-Item -LiteralPath $stdout, $stderr
            if ($null -ne $process) { $process.Dispose() }
        }
    }
}
$report = [ordered]@{ os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription; architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString(); cpu = $env:PROCESSOR_IDENTIFIER; rustc = (rustc --version); binary = $binary; git_base = (git rev-parse HEAD); dirty = [bool](git status --porcelain); rss_method = 'Windows PeakWorkingSet64 sampled every 20 ms; ready sample includes allocator reuse from fixture generation'; measurements = $measurements }
$report.label = $Label
$report.suite = $Suite
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding utf8
