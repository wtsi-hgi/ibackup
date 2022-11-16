module github.com/wtsi-hgi/ibackup

go 1.19

require (
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/dustin/go-humanize v1.0.0
	github.com/gammazero/workerpool v1.1.3
	github.com/gin-gonic/gin v1.8.1
	github.com/go-ldap/ldap/v3 v3.4.4
	github.com/go-resty/resty/v2 v2.7.0
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/olekukonko/tablewriter v0.0.5
	github.com/rs/zerolog v1.28.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.6.1
	github.com/ugorji/go/codec v1.2.7
	github.com/wtsi-hgi/go-authserver v1.0.2
	github.com/wtsi-npg/extendo/v2 v2.3.1-0.20221006092230-de9e05a07c64
	github.com/wtsi-npg/logshim v1.3.0
	github.com/wtsi-npg/logshim-zerolog v1.3.0
	github.com/wtsi-ssg/wrstat/v3 v3.1.2
	go.etcd.io/bbolt v1.3.6
	golang.org/x/term v0.2.0
)

require (
	github.com/Azure/go-ntlmssp v0.0.0-20220621081337-cb9428e4ac1e // indirect
	github.com/appleboy/gin-jwt/v2 v2.9.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/gammazero/deque v0.2.0 // indirect
	github.com/gin-contrib/secure v0.0.1 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.4 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.1 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/gorilla/sessions v1.2.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/karrick/godirwalk v1.17.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/jwx v1.2.25 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/okta/okta-jwt-verifier-golang v1.3.1 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pelletier/go-toml/v2 v2.0.5 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rivo/uniseg v0.3.4 // indirect
	github.com/smartystreets/assertions v1.13.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/thanhpk/randstr v1.0.4 // indirect
	github.com/wtsi-ssg/wr v0.5.5 // indirect
	golang.org/x/crypto v0.2.0 // indirect
	golang.org/x/net v0.2.0 // indirect
	golang.org/x/oauth2 v0.2.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.2.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/tylerb/graceful.v1 v1.2.15 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

// we need to specify these due to github.com/VertebrateResequencing/wr's deps
replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0
