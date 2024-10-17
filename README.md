# workspace-provider

There are two providers that can be used to create and manage workspaces: directory and S3.

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
Additionally, you should also set the `WORKSPACE_PROVIDER_S3_BASE_ENDPOINT` environment variable to the endpoint of your provider. For example, if you are using Cloudflare R2, you can set `WORKSPACE_PROVIDER_S3_BASE_ENDPOINT` to `https://<ACCOUND_ID>.r2.cloudflarestorage.com`.