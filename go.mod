module github.com/wtsi-hgi/ibackup

go 1.19

require (
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/rs/zerolog v1.28.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.5.0
	github.com/wtsi-npg/extendo/v2 v2.3.1-0.20221006092230-de9e05a07c64
	github.com/wtsi-npg/logshim v1.3.0
	github.com/wtsi-npg/logshim-zerolog v1.3.0
)

require (
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
)

// we need to specify these due to github.com/VertebrateResequencing/wr's deps
replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0
