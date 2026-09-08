param(
    [Parameter(Mandatory=$true)][string]$TestBinary,
    [string]$Output = 'update-prepare-benchmark.json'
)
$ErrorActionPreference = 'Stop'
$binary = (Resolve-Path -LiteralPath $TestBinary).Path
$testName = 'table::operations::update_prepare::tests::preparation_memory_benchmark'
$measurements = @()
foreach ($rows in @(16384, 65536)) {
    foreach ($mode in @('shuffled', 'concentrated')) {
        $env:TST_UPDATE_TARGET_ROWS = "$rows"
        $env:TST_UPDATE_ROWS = "$($rows / 4)"
        $env:TST_UPDATE_SORT_BYTES = '65536'
        $env:TST_UPDATE_MODE = $mode
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
            $measurement = [ordered]@{ parameters = $ready; result = $result; peak_rss_bytes = $peakRss; sampled_ready_rss_bytes = $readyRss; peak_handles = $peakHandles; sampled_ready_handles = $readyHandles }
            $measurements += $measurement
            $measurement | ConvertTo-Json -Depth 5 -Compress
        } finally {
            Remove-Item -LiteralPath $stdout, $stderr
            if ($null -ne $process) { $process.Dispose() }
        }
    }
}
$report = [ordered]@{ os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription; architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString(); cpu = $env:PROCESSOR_IDENTIFIER; rustc = (rustc --version); binary = $binary; git_base = (git rev-parse HEAD); dirty = [bool](git status --porcelain); rss_method = 'Windows PeakWorkingSet64 sampled every 20 ms; ready sample includes allocator reuse from fixture generation'; measurements = $measurements }
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding utf8
