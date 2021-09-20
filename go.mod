module github.com/nitin-deamon/azure-storage-azcopy/v10

replace github.com/Azure/azure-storage-azcopy/v10/jobsAdmin => ./

go 1.17

require (
	cloud.google.com/go/storage v1.16.1
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-azcopy/v10 v10.12.1
	github.com/Azure/azure-storage-azcopy/v10/jobsAdmin v0.0.0-00010101000000-000000000000
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/Azure/azure-storage-file-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.9.16
	github.com/JeffreyRichter/enum v0.0.0-20180725232043-2567042f9cda
	github.com/danieljoos/wincred v1.1.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/google/uuid v1.3.0
	github.com/hillu/go-ntdll v0.0.0-20210404124636-a6f426aa8d92
	github.com/jiacfan/keychain v0.0.0-20180920053336-f2c902a3d807
	github.com/jiacfan/keyctl v0.3.1
	github.com/mattn/go-ieproxy v0.0.1
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.2.1
	golang.org/x/crypto v0.0.0-20210920023735-84f357641f63
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210917161153-d61c044b1678
	google.golang.org/api v0.57.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

require (
	cloud.google.com/go v0.94.1 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/go-ini/ini v1.62.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/googleapis/gax-go/v2 v2.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/net v0.0.0-20210805182204-aaa1db679c0d // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210903162649-d08c68adba83 // indirect
	google.golang.org/grpc v1.40.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
