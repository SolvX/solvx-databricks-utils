Write-Host "Building wheel..."
python -m build --wheel

$wheel = Get-ChildItem dist\*.whl | Select-Object -Last 1

Write-Host "Installing $($wheel.Name)..."
pip install --force-reinstall $wheel.FullName

Write-Host "Done!"
