<#
.SYNOPSIS
Creates the storage for the Terraform remote state. Run once, before
`terraform init -backend-config=backend.hcl -migrate-state` in infra/terraform.

.DESCRIPTION
The state account is deliberately not managed by infra/terraform: a configuration should not
keep its state in a resource it can destroy, and `terraform destroy` removes rg-ddlake-dev with
everything in it. So the state gets its own resource group, created here with the Azure CLI.

Settings: StorageV2, LRS, no hierarchical namespace (blob versioning is not available with it),
blob versioning and 7-day soft delete for blobs and containers, shared keys and public access
off, HTTPS and TLS 1.2 only. Terraform reaches it with Entra ID (use_azuread_auth), so the
signed-in user gets "Storage Blob Data Contributor" on this account only.

Cost: the state is about 50 KB and a plan makes a few dozen requests: well under EUR 0.01 a
month. The resource group is outside the EUR 1 budget of rg-ddlake-dev (docs/adr/0006).

The script is idempotent: existing resources are left as they are.

.EXAMPLE
az login --tenant <tenant id>
./infra/bootstrap/create_tfstate.ps1 -SubscriptionId <id> -StorageAccountName <globally unique name>
#>
param(
    [Parameter(Mandatory = $true)] [string] $SubscriptionId,
    [Parameter(Mandatory = $true)] [ValidatePattern('^[a-z0-9]{3,24}$')] [string] $StorageAccountName,
    [string] $ResourceGroup = 'rg-ddlake-tfstate',
    [string] $Location = 'northeurope',
    [string] $Container = 'tfstate'
)
$ErrorActionPreference = 'Stop'

# Runs az and stops on a non-zero exit code (native commands do not throw on their own).
# On Windows az is a batch file (az.cmd), so arguments must not contain parentheses without
# spaces: PowerShell passes them unquoted and cmd.exe misreads them. Count results here instead
# of with JMESPath length(@).
function Invoke-Az {
    $output = az @args
    if ($LASTEXITCODE -ne 0) { throw "az $($args[0..2] -join ' ') ... failed with exit code $LASTEXITCODE" }
    return $output
}

$tags = @('project=ddlake', 'purpose=terraform-state', 'managed_by=az-cli')

Write-Host "1/5 subscription and resource group $ResourceGroup"
Invoke-Az account set --subscription $SubscriptionId | Out-Null
Invoke-Az group create --name $ResourceGroup --location $Location --tags @tags --output none

Write-Host "2/5 storage account $StorageAccountName"
$existing = @(Invoke-Az storage account list --resource-group $ResourceGroup `
    --query "[?name=='$StorageAccountName'].name" --output tsv | Where-Object { $_ })
if ($existing.Count -gt 0) {
    Write-Host '    exists, left as it is'
} else {
    Invoke-Az storage account create --name $StorageAccountName --resource-group $ResourceGroup `
        --location $Location --kind StorageV2 --sku Standard_LRS --hns false `
        --min-tls-version TLS1_2 --https-only true --allow-blob-public-access false `
        --allow-shared-key-access false --tags @tags --output none
}

Write-Host '3/5 blob versioning and soft delete (7 days)'
Invoke-Az storage account blob-service-properties update --account-name $StorageAccountName `
    --resource-group $ResourceGroup --enable-versioning true `
    --enable-delete-retention true --delete-retention-days 7 `
    --enable-container-delete-retention true --container-delete-retention-days 7 --output none

Write-Host '4/5 Storage Blob Data Contributor for the signed-in user on this account'
$me = Invoke-Az ad signed-in-user show --query id --output tsv
$scope = Invoke-Az storage account show --name $StorageAccountName --resource-group $ResourceGroup `
    --query id --output tsv
$assigned = @(Invoke-Az role assignment list --assignee $me --scope $scope `
    --role 'Storage Blob Data Contributor' --query '[].id' --output tsv | Where-Object { $_ })
if ($assigned.Count -eq 0) {
    Invoke-Az role assignment create --assignee-object-id $me --assignee-principal-type User `
        --role 'Storage Blob Data Contributor' --scope $scope --output none
}

# A new role assignment can take a few minutes to apply to data-plane requests.
Write-Host "5/5 container $Container (retries while the role assignment propagates)"
for ($attempt = 1; $attempt -le 10; $attempt++) {
    az storage container create --name $Container --account-name $StorageAccountName `
        --auth-mode login --output none
    if ($LASTEXITCODE -eq 0) { break }
    if ($attempt -eq 10) { throw 'container could not be created; check the role assignment' }
    Write-Host "    not yet (attempt $attempt of 10), waiting 30 s"
    Start-Sleep -Seconds 30
}

Write-Host ''
Write-Host 'Done. Put these values in infra/terraform/backend.hcl (see backend.hcl.example):'
Write-Host "  resource_group_name  = `"$ResourceGroup`""
Write-Host "  storage_account_name = `"$StorageAccountName`""
Write-Host "  container_name       = `"$Container`""
