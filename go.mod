module github.com/nitin-deamon/azure-storage-azcopy/v10

replace github.com/Azure/azure-storage-azcopy/v10/jobsAdmin => ./

replace github.com/Azure/azure-storage-azcopy/v10/common => ./

replace github.com/Azure/azure-storage-azcopy/v10/ste => ./

replace github.com/Azure/azure-storage-azcopy/v10/cmd => ./

go 1.16

require (
	cloud.google.com/go/storage v1.16.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-azcopy/v10 v10.12.1
	github.com/Azure/azure-storage-azcopy/v10/jobsAdmin v0.0.0-00010101000000-000000000000
	github.com/Azure/azure-storage-blob-go v0.13.1-0.20210823171415-e7932f52ad61
	github.com/Azure/azure-storage-file-go v0.6.1-0.20201111053559-3c1754dc00a5
	github.com/Azure/go-autorest/autorest/adal v0.9.14
	github.com/JeffreyRichter/enum v0.0.0-20180725232043-2567042f9cda
	github.com/danieljoos/wincred v1.1.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/google/uuid v1.3.0
	github.com/hillu/go-ntdll v0.0.0-20210404124636-a6f426aa8d92
	github.com/jiacfan/keychain v0.0.0-20180920053336-f2c902a3d807
	github.com/jiacfan/keyctl v0.3.1
	github.com/mattn/go-ieproxy v0.0.1
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.2.1
	golang.org/x/crypto v0.0.0-20210812204632-0ba0e8f03122
	golang.org/x/oauth2 v0.0.0-20210810183815-faf39c7919d5
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210809222454-d867a43fc93e
	google.golang.org/api v0.53.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)
