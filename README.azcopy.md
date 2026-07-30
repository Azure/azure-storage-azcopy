# AzCopy

[AzCopy](https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-v10) is a high-performance, command-line utility for copying data to and from Azure Storage (Blob, File, and Table) and other supported endpoints.

These images package the official AzCopy binary on top of [Azure Linux (CBL-Mariner)](https://mcr.microsoft.com/product/cbl-mariner/base/core/about) for a small, secure, and regularly patched base.

## Supported tags and architectures

Images are published per architecture under the `azcopy/linux/<arch>` path:

| Repository | Architecture |
| --- | --- |
| `mcr.microsoft.com/azcopy/linux/amd64` | linux/amd64 |
| `mcr.microsoft.com/azcopy/linux/arm64` | linux/arm64 |

Each repository provides:

- `:<version>` — an immutable tag pinned to a specific release (for example `:10.32.22`).
- `:latest` — always points to the newest released version.

> Pin to a `:<version>` tag for reproducible builds; use `:latest` to automatically stay on the most recent release.

## Pull the image

```bash
# amd64
docker pull mcr.microsoft.com/azcopy/linux/amd64:latest

# arm64
docker pull mcr.microsoft.com/azcopy/linux/arm64:latest
```

## Run AzCopy

The container's working directory is `/azcopy`. Mount a local directory there to share files and AzCopy plan/log files with the host.

```bash
docker run --rm -it \
  -v /local/path/to/mount:/azcopy \
  mcr.microsoft.com/azcopy/linux/amd64:latest \
  azcopy copy <source> <destination>
```

Check the version:

```bash
docker run --rm mcr.microsoft.com/azcopy/linux/amd64:latest azcopy --version
```

## Authentication

AzCopy supports several authentication methods, including SAS tokens, Microsoft Entra ID, and managed identity. For example, to authenticate with Microsoft Entra ID inside the container:

```bash
docker run --rm -it \
  -e AZCOPY_AUTO_LOGIN_TYPE=DEVICE \
  -v /local/path/to/mount:/azcopy \
  mcr.microsoft.com/azcopy/linux/amd64:latest \
  azcopy copy <source> <destination>
```

See [Authorize AzCopy](https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-authorize-azure-active-directory) for all supported options.

## Documentation and support

- Documentation: https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-v10
- Source code: https://github.com/Azure/azure-storage-azcopy
- Report issues: https://github.com/Azure/azure-storage-azcopy/issues

## License

AzCopy is released under the [MIT License](https://github.com/Azure/azure-storage-azcopy/blob/main/LICENSE). By using these images you agree to the licenses of any software contained within them.
