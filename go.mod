module github.com/kkguan/p2pdb-store

go 1.17

replace github.com/kkguan/p2pdb-store => ../p2pdb-store

require (
	github.com/caarlos0/env/v6 v6.9.1
	github.com/cespare/xxhash v1.1.0
	github.com/dolthub/vitess v0.0.0-20211215165926-1490f8c93e81
	github.com/favframework/debug v0.0.0-20150708094948-5c7e73aafb21
	github.com/go-kit/kit v0.12.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocraft/dbr/v2 v2.7.3
	github.com/google/uuid v1.3.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/joho/godotenv v1.4.0
	github.com/kkguan/p2pdb-server v0.1.1
	github.com/lestrrat-go/strftime v1.0.5
	github.com/mitchellh/hashstructure v1.1.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pmezard/go-difflib v1.0.0
	github.com/shopspring/decimal v1.3.1
	github.com/sirupsen/logrus v1.8.1
	github.com/src-d/go-oniguruma v1.1.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/text v0.3.7
	gopkg.in/src-d/go-errors.v1 v1.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/mattn/go-sqlite3 v1.14.11 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/net v0.0.0-20210917221730-978cfadd31cf // indirect
	golang.org/x/sys v0.0.0-20210917161153-d61c044b1678 // indirect
	google.golang.org/genproto v0.0.0-20210917145530-b395a37504d4 // indirect
	google.golang.org/grpc v1.40.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
