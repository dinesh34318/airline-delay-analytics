# ==============================================================================
# run_dashboard.ps1
# PowerShell runner for the Stage 11 Interactive Shiny Dashboard
# ==============================================================================

$Rscript = "C:\Program Files\R\R-4.3.2\bin\Rscript.exe"
$ScriptPath = Join-Path $PSScriptRoot "run_dashboard.R"

& $Rscript $ScriptPath
