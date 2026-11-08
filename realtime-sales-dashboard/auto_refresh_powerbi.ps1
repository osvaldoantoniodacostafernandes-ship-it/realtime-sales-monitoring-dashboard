# ==============================================================================
# auto_refresh_powerbi.ps1
# Atualizasaun Automátika ba Power BI Desktop (Lian Tetun)
# Hanehan refresh (Ctrl + R / F5) automátiku kada sekundu 4 ba Power BI Desktop
# ==============================================================================

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "🔄 ATUALIZASAUN AUTOMATIKA POWER BI DESKTOP (DEMO LIVE)" -ForegroundColor Green
Write-Host "Intervalu : Kada Sekundu 4 Automátiku" -ForegroundColor Yellow
Write-Host "Atu Para  : Hanehan Ctrl + C" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan

$wshell = New-Object -ComObject WScript.Shell

while ($true) {
    Start-Sleep -Seconds 4
    $activated = $wshell.AppActivate("Power BI Desktop")
    if (-not $activated) {
        $activated = $wshell.AppActivate("Power BI")
    }
    
    if ($activated) {
        Start-Sleep -Milliseconds 100
        $wshell.SendKeys("^r")
        $wshell.SendKeys("{F5}")
        $timestamp = (Get-Date).ToString("HH:mm:ss")
        Write-Host "[$timestamp] ⚡ Power BI Desktop Atualiza ona ho Susesu!" -ForegroundColor Green
    } else {
        $timestamp = (Get-Date).ToString("HH:mm:ss")
        Write-Host "[$timestamp] ⏳ Hein hela janela Power BI Desktop loke..." -ForegroundColor Gray
    }
}
