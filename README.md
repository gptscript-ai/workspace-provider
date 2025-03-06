# workspace-provider

There are three providers that can be used to create and manage workspaces: directory, S3, and Azure.

## Directory

The directory provider provides a directory-based workspace. This provider is used by default.

## S3

The S3 provider provides a S3-based workspace.

This provider supports loading the default AWS configuration. You can control this configuration using the following environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

You must set the following environment variables:
- `WORKSPACE_PROVIDER_S3_BUCKET`

### Usage with S3-compatible providers (e.g. Cloudflare R2)

You can use the above referenced AWS environment variables to configure the S3 provider, setting the value of the environment variable to the corresponding value from your provider.
Additionally, you should also set the `WORKSPACE_PROVIDER_S3_BASE_ENDPOINT` environment variable to the endpoint of your provider. For example, if you are using Cloudflare R2, you can set `WORKSPACE_PROVIDER_S3_BASE_ENDPOINT` to `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`.

## Azure

The Azure provider provides an Azure Blob Storage-based workspace.

### Setup

1. Create an Azure Storage Account in the [Azure Portal](https://portal.azure.com)
2. Create a container in your storage account
3. Get the connection string from your storage account (under "Access keys")

### Configuration

You must set the following environment variables:
- `WORKSPACE_PROVIDER_AZURE_CONTAINER` - The name of your Azure Storage container
- `WORKSPACE_PROVIDER_AZURE_CONNECTION_STRING` - The connection string for your Azure Storage account

For example:
```bash
export WORKSPACE_PROVIDER_AZURE_CONTAINER="your-container-name"
export WORKSPACE_PROVIDER_AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```
