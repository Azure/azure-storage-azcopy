module github.com/Azure/azure-storage-azcopy/v10

replace (
	github.com/Azure/azure-storage-blob-go => C:\Users\adreed\go\src\github.com\Azure\azure-storage-blob-go
)

require (
	cloud.google.com/go/iam v0.3.0 // indirect
	cloud.google.com/go/storage v1.21.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.13.1-0.20210914164749-2d6cd3e07548
	github.com/Azure/azure-storage-file-go v0.6.1-0.20201111053559-3c1754dc00a5
	github.com/Azure/go-autorest/autorest/adal v0.9.18
	github.com/JeffreyRichter/enum v0.0.0-20180725232043-2567042f9cda
	github.com/danieljoos/wincred v1.1.2
	github.com/go-ini/ini v1.66.4 // indirect
	github.com/golang-jwt/jwt/v4 v4.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/google/uuid v1.3.0
	github.com/hillu/go-ntdll v0.0.0-20220217145204-be7b5318100d
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mattn/go-ieproxy v0.0.3
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/spf13/cobra v1.4.0
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/wastore/keychain v0.0.0-20180920053336-f2c902a3d807
	github.com/wastore/keyctl v0.3.1
	golang.org/x/crypto v0.0.0-20220314234724-5d542ad81a58
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220310020820-b874c991c1a5
	google.golang.org/api v0.72.0
	google.golang.org/genproto v0.0.0-20220314164441-57ef72a4c106 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

go 1.16
